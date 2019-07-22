package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest


import scala.collection.mutable


/**
  * PRT的全称：PearsonRandomTrees
  *
  * @param pearsonThreashold
  * 计算Pearson系数时，最低用户之间共同拥有的元素个数。若相同元素个数的阀值，低于该阀值，相似度为0.
  * @param numNearestUsers
  * Pearson相似度最大的前N个用户
  **/
case class PRTAlgorithmParams(pearsonThreashold: Int, numNearestUsers: Int,numUserLikeMovies:Int) extends Params


/** 功能：
  * 以用户与用户之间Pearson相似度为基础，通过随机森林进行过滤的综合算法。
  *
  * @param ap 该算法在engine.json中设置的参数
  **/
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
    val userLikesAndNearstPearson = new Pearson(ap.pearsonThreashold, ap.numNearestUsers,ap.numUserLikeMovies).getPearsonNearstUsers(userRatings)

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

    val numTrees = 5
    val featureSubsetStrategy: String = "auto"
    val impurity: String = "gini"
    val maxDepth: Int = 5
    val maxBins: Int = 100


    val model = RandomForest.trainClassifier(trainingData, numClass, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    new PRTModel(sc.parallelize(userRatings.toSeq),
      sc.parallelize(userLikesAndNearstPearson._2.toSeq), //最近的N个用户列表
      sc.parallelize(userLikesAndNearstPearson._1.toSeq), //用户的喜欢列表
      model)

  }


  override def predict(model: PRTModel, query: Query): PredictedResult = {
    val uMap = model.userMap.collectAsMap()
    if (!uMap.contains(query.user)) {
      //该用户没有过评分记录，返回空值
      logger.warn(s"该用户没有过评分记录，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //1.获取当前要推荐用户的Pearson值最大的用户列表
    val userPearson = model.userNearestPearson.collectAsMap()
    if (!userPearson.contains(query.user)) {
      //该用户没有对应的Pearson相似用户
      logger.warn(s"该用户没有相似的用户，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //2.所有用户最喜欢的前N部电影
    val userLikes = model.userLikesBeyondMean.collectAsMap()

    //当前用户已经观看过的列表
    val sawItem = uMap(query.user).map(r => (r.item, r.rating)).toMap

    //存储结果的列表
    val pearsonResult = new mutable.HashMap[String, Double]()
    //与当前查询用户相似度最高的用户，其观看过的且查询用户未看过的电影列表。
    userPearson(query.user).foreach(r => {
      //r._1 //相似的userID
      // r._2 //相似度
      if (userLikes.contains(r._1)) {
        //r._1用户有最喜欢的电影记录
        userLikes(r._1).map(r1 => {
          //r1.item
          //r1.rating
          if (!sawItem.contains(r1.item)) {
            //当前用户未观看过的电影r1.item
            if (pearsonResult.contains(r1.item)) {
              //这是已经在推荐列表中
              pearsonResult.update(r1.item, pearsonResult(r1.item) + r1.rating * r._2)
            } else {
              pearsonResult.put(r1.item, r1.rating * r._2)
            }
          }
        })
      }
    })


    val randomModel = model.randomForestModel
    val filtedResult = pearsonResult.filter(r => {
      val v = Vectors.dense(query.user.toDouble, r._1.toDouble)
      randomModel.predict(v) == 1.0
    })


    //排序取TopN
    val preResult = filtedResult.map(r => (r._1, r._2)).toList.sortBy(_._2).reverse.take(query.num).map(r => (r._1, r._2))

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum
    val weight = 1
    val returnResult = pearsonResult.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    })

    //排序，返回结果
    PredictedResult(returnResult.toArray)
  }
}
