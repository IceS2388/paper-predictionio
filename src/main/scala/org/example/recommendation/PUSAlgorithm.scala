package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * PUS的全称：PearsonUserCorrelationSimilarity
  **/

/**
  * @param pearsonThreasholds 计算Pearson系数时，最低用户之间共同拥有的元素个数。若相同元素个数的阀值，低于该阀值，相似度为0.
  * @param topNLikes          Pearson相似度最大的前N个用户
  **/
case class PUSAlgorithmParams(pearsonThreasholds: Int, topNLikes: Int) extends Params


/** 功能：
  * 实现用户Pearson相似度算法。
  *
  * @param ap 该算法在engine.json中设置的参数
  **/
class PUSAlgorithm(val ap: PUSAlgorithmParams) extends PAlgorithm[PreparedData, PUSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]


  /** *
    * 训练阶段:
    *  1.根据输入的数据，获取用户与用户之间的Pearson相似度。
    *  2.模型中存储前N个最相似的用户和Pearson相似度
    *  3.根据模型选择前N个用户，筛选其超过平均值的电影评分。
    */
  override def train(sc: SparkContext, data: PreparedData): PUSModel = {

    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    //1.转换为HashMap,方便计算Pearson相似度,这是个昂贵的操作
    val userRatings: RDD[(String, Iterable[Rating])] = data.ratings.groupBy(r => r.user)


    //2.获取用户的ID
    val users = data.ratings.map(r => r.user).distinct().sortBy(_.toInt, true)

    val userNearestPearson = new mutable.HashMap[String, List[(String, Double)]]()
    for {
      u1 <- users
    } {
      val maxPearson: mutable.Map[String, Double] = mutable.HashMap.empty
      for {u2 <- users
           if (u1 < u2)} {
        val ps = getPearson(u1, u2, userRatings)
        if (ps > 0) {
          //有用的相似度
          if (maxPearson.size < ap.topNLikes) {
            maxPearson.put(u2, ps)
          } else {
            val min_p = maxPearson.map(r => (r._1, r._2)).minBy(r => r._2)
            if (ps > min_p._2) {
              maxPearson.remove(min_p._1)
              maxPearson.put(u2, ps)
            }

          }
        }
      }

      userNearestPearson.put(u1, maxPearson.toList.sortBy(_._2).reverse)
    }

    //3.从用户的观看记录中选择用户喜欢的电影,用于后续的用户与用户之间的推荐
    val userLikesBeyondMean: RDD[(String, List[Rating])] = userRatings.map(r => {

      //当前用户的平均评分
      val sum = r._2.map(r => r.rating).sum
      val count = r._2.size

      //用户浏览的小于numNearst，全部返回
      val userLikes = if (count < ap.topNLikes) {
        //排序后，直接返回
        r._2.toList.sortBy(_.rating).reverse
      } else {
        val mean = sum / count
        r._2.filter(t => t.rating > mean).toList.sortBy(_.rating).reverse.take(ap.topNLikes)
      }

      (r._1, userLikes)
    })

    //4.训练RandomForestModel
    //4.1 计算用户的平均分
    val userMean = userRatings.map(r => {
      val sum = r._2.toSeq.map(r2 => r2.rating).sum
      val size = r._2.size
      (r._1, sum / size)
    }).collectAsMap()

    //4.2 处理处理数据格式
    val trainingData = data.ratings.map(r => {
      val like = if (r.rating > userMean(r.user)) 1.0 else 0D
      LabeledPoint(like, Vectors.dense(r.user.toInt, r.item.toInt))
    })

    //4.3 准备模型参数
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

    new PUSModel(userRatings, sc.parallelize(userNearestPearson.toSeq), userLikesBeyondMean, model)

  }

  /**
    * Pearson相似度计算公式:
    * r=sum((x-x_mean)*(y-y_mean))/(Math.pow(sum(x-x_mean),0.5)*Math.pow(sum(y-y_mean),0.5))
    * 要求，两者的共同因子必须达到阀值。默认为10
    **/
  private def getPearson(userid1: String,
                         userid2: String,
                         userHashRatings: RDD[(String, Iterable[Rating])]): Double = {

    val user1DataRDD: RDD[(String, Iterable[Rating])] = userHashRatings.filter(r => r._1 == userid1)
    val user2DataRDD: RDD[(String, Iterable[Rating])] = userHashRatings.filter(r => r._1 == userid2)

    if (!(user1DataRDD.count() == 1 && user1DataRDD.first()._2.nonEmpty &&
      user2DataRDD.count() == 1 && user2DataRDD.first()._2.nonEmpty)) {
      //不相关
      return 0D
    }

    val user1Data = user1DataRDD.first()._2
    val user2Data = user2DataRDD.first()._2

    //1.求u1与u2共同的物品ID
    val comItemSet = user1Data.map(r => r.item).toSet.intersect(user2Data.map(r => r.item).toSet)
    if (comItemSet.size < ap.pearsonThreasholds) {
      //小于共同物品的阀值，直接退出
      return 0D
    }

    val user1ComData = user1Data.filter(r => comItemSet.contains(r.item)).map(r => (r.item, r.rating)).toMap
    val user2ComData = user2Data.filter(r => comItemSet.contains(r.item)).map(r => (r.item, r.rating)).toMap

    //2.把共同物品转变为对应的评分
    val comItems = comItemSet.map(r => (r, (user1ComData(r), user2ComData(r))))

    //计算平均值和标准差
    val count = comItems.size
    val sum1 = comItems.map(item => item._2._1).reduce(_ + _)
    val sum2 = comItems.map(item => item._2._2).reduce(_ + _)

    //平均值
    val x_mean = sum1 / count
    val y_mean = sum2 / count

    //标准差
    var xy = 0D
    var x_var = 0D
    var y_var = 0D
    comItems.foreach(i => {

      val x_vt = i._2._1 - x_mean
      val y_vt = i._2._2 - y_mean
      xy += x_vt * y_vt

      x_var += x_vt * x_vt
      y_var += y_vt * y_vt
    })

    //Pearson系数
    xy / (Math.pow(x_var, 0.5) * Math.pow(y_var, 0.5))
  }


  override def predict(model: PUSModel, query: Query): PredictedResult = {
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
    val sawItem = uMap.get(query.user).get.map(r => (r.item, r.rating)).toMap

    //存储结果的列表
    val pearsonResult = new mutable.HashMap[String, Double]()
    //与当前查询用户相似度最高的用户，其观看过的且查询用户未看过的电影列表。
    userPearson.get(query.user).get.foreach(r => {
      //r._1 //相似的userID
      // r._2 //相似度
      if (userLikes.contains(r._1)) {
        //r._1用户有最喜欢的电影记录
        userLikes.get(r._1).get.map(r1 => {
          //r1.item
          //r1.rating
          if (!sawItem.contains(r1.item)) {
            //当前用户未观看过的电影r1.item
            if (pearsonResult.contains(r1.item)) {
              //这是已经在推荐列表中
              pearsonResult.update(r1.item, pearsonResult.get(r1.item).get + r1.rating * r._2)
            } else {
              pearsonResult.put(r1.item, r1.rating * r._2)
            }
          }
        })
      }
    })


    val randomModel = model.randomForestModel
    val filted = pearsonResult.filter(r => {
      val v = Vectors.dense(query.user.toDouble, r._1.toDouble)
      randomModel.predict(v) == 1.0
    })


    //排序取TopN
    val preResult = filted.map(r => (r._1, r._2)).toList.sortBy(_._2).reverse.take(query.num).map(r => (r._1, r._2))

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum
    val PUSWeight = 2
    val returnResult = pearsonResult.map(r => {
      ItemScore(r._1, r._2 / sum * PUSWeight)
    })

    //排序，返回结果
    PredictedResult(returnResult.toArray)
  }
}
