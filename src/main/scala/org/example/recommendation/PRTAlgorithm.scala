package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD


/**
  * PRT的全称：PearsonRandomTrees
  *
  * @param pearsonThreashold
  * 计算Pearson系数时，最低用户之间共同拥有的元素个数。若相同元素个数的阀值，低于该阀值，相似度为0.
  * @param numNearestUsers
  * Pearson相似度最大的前N个用户
  **/
case class PRTAlgorithmParams(
                               pearsonThreashold: Int = 10,
                               numNearestUsers: Int = 60,
                               numUserLikeMovies: Int = 100,
                               numTrees: Int = 5,
                               featureSubsetStrategy: String = "auto",
                               impurity: String = "gini",
                               maxDepth: Int = 5,
                               maxBins: Int = 100
                             ) extends Params


/** 功能：
  * 以用户与用户之间Pearson相似度为基础，通过随机森林进行过滤的综合算法。
  *
  * @param ap 该算法在engine.json中设置的参数
  * */
class PRTAlgorithm(val ap: PRTAlgorithmParams) extends PAlgorithm[PreparedData, PRTModel, Query, PredictedResult] {

  @transient lazy val logger: Logger = Logger[this.type]

  /**
    * 1.以Pearson相似度提供初步被筛选的数据。
    * 2.训练随机森林的分类模型
    **/
  override def train(sc: SparkContext, data: PreparedData): PRTModel = {

    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    //1.转换为HashMap,方便计算Pearson相似度,这是个昂贵的操作
    val userRatings: Map[String, Iterable[Rating]] = data.ratings.groupBy(r => r.user).collectAsMap().toMap

    //2.计算用户与用户之间Pearson系数，并返回
    // 用户观看过后喜欢的列表(列表长度需要限制一下) 和 pearson系数最大的前TopN个用户的列表
    val userLikesAndNearstPearson = new Pearson(ap.pearsonThreashold, ap.numNearestUsers, ap.numUserLikeMovies).getPearsonNearstUsers(userRatings)

    //3.训练RandomForestModel
    //3.1 计算用户的平均分
    val userMean = userRatings.map(r => {
      val sum = r._2.toSeq.map(r2 => r2.rating).sum
      val size = r._2.size
      (r._1, sum / size)
    })

    //3.2 处理处理数据格式
    val trainingData = data.ratings.map(r => {
      val like = if (r.rating > userMean(r.user)) 1.0 else 0D
      LabeledPoint(like, Vectors.dense(r.user.toInt, r.item.toInt))
    })

    //3.3 准备模型参数
    //分类数目
    val numClass = 2
    //设定输入数据格式
    val categoricalFeaturesInfo = Map[Int, Int]()

    val model = RandomForest.trainClassifier(trainingData, numClass, categoricalFeaturesInfo, ap.numTrees, ap.featureSubsetStrategy.toLowerCase(), ap.impurity.toLowerCase(), ap.maxDepth, ap.maxBins)

    new PRTModel(sc.parallelize(userRatings.toSeq),
      sc.parallelize(userLikesAndNearstPearson._2.toSeq), //最近的N个用户列表
      sc.parallelize(userLikesAndNearstPearson._1.toSeq), //用户的喜欢列表
      model)

  }


  override def predict(model: PRTModel, query: Query): PredictedResult = {


    //1.判断当前用户有没有看过电影
    val currentUserRDD = model.userMap.filter(r => r._1 == query.user)
    if (currentUserRDD.count() == 0) {
      //该用户没有过评分记录，返回空值
      logger.warn(s"该用户:${query.user}没有过评分记录，无法生成推荐！")
      return PredictedResult(Array.empty)
    }

    //2.获取当前用户的Pearson值最大的用户列表
    //2.1 判断有没有列表
    val similaryUers = model.userNearestPearson.filter(r => r._1 == query.user)
    if (similaryUers.count() == 0) {
      //该用户没有最相似的Pearson用户列表
      logger.warn(s"该用户:${query.user}没有Pearson相似用户列表，无法生成推荐！")
      return PredictedResult(Array.empty)
    }

    val pUsersMap: collection.Map[String, Double] = similaryUers.flatMap(r => r._2).collectAsMap()
    //这是当前查询用户已经看过的电影
    val userSawMovie = currentUserRDD.flatMap(r => r._2.map(rr => (rr.item, rr.rating))).collectAsMap()


    //3. 从用户喜欢的电影列表，获取相似度用户看过的电影
    //原先的版本是从用户看过的列表中选择
    val result: RDD[(String, Double)] = model.userLikesBeyondMean.filter(r => {
      // r._1 用户ID
      //3.1 筛选相关用户看过的电影列表
      pUsersMap.contains(r._1)
    }).flatMap(r => {
      //r: (String, Iterable[Rating])
      //3.2 生成每一个item的积分
      r._2.map(r2 => {
        (r2.item, r2.rating * pUsersMap(r._1))
      })
    }).filter(r => {
      //r._1 itemID
      // 3.3 过滤掉用户已经看过的电影
      !userSawMovie.contains(r._1)
    }).reduceByKey(_ + _)


    if (result.count() == 0) return PredictedResult(Array.empty)

    val randomModel = model.randomForestModel
    val filtedResult = result.filter(r => {
      val v = Vectors.dense(query.user.toInt, r._1.toInt)
      randomModel.predict(v) == 1.0
    })

    logger.info(s"筛选过后复合条件的物品数量为：${filtedResult.count()}")
    //排序取TopN
    val preResult = filtedResult.sortBy(_._2, false).take(query.num)

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum

    if (sum == 0) return PredictedResult(Array.empty)

    val weight = 1
    val returnResult = preResult.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    })

    //排序，返回结果
    PredictedResult(returnResult)
  }

  override def batchPredict(m: PRTModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    qs.map(r => {
      //r._1
      (r._1, predict(m, r._2))
    })
  }
}
