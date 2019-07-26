package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Author:IceS
  * Date:2019-07-19 21:11:37
  * Description:
  * NONE
  */
case class NBAlgorithmParams(pearsonThreashold: Int=10, numNearestUsers: Int=60, numUserLikeMovies:Int=100) extends Params

class NBAlgorithm(val ap: NBAlgorithmParams) extends PAlgorithm[PreparedData, NBModel, Query, PredictedResult] {

  @transient lazy val logger: Logger = Logger[this.type]

  override def train(sc: SparkContext, data: PreparedData): NBModel = {
    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    //1.转换为HashMap,方便计算Pearson相似度,这是个昂贵的操作
    val userRatings: Map[String, Iterable[Rating]] = data.ratings.groupBy(r => r.user).collectAsMap().toMap

    //2.计算用户与用户之间Pearson系数，并返回用户观看过后喜欢的列表和pearson系数最大的前TopN个用户的列表
    val userLikesAndNearstPearson = new SimilarityFactor(ap.pearsonThreashold, ap.numNearestUsers,ap.numUserLikeMovies).getNearstUsers(userRatings)

    //3.朴素贝叶斯
    //3.1 计算用户的平均分
    val userMean: Map[String, Double] = userRatings.map(r => {
      val sum = r._2.toSeq.map(r2 => r2.rating).sum
      val size = r._2.size
      (r._1, sum / size)
    })

    //3.2 处理处理数据格式
    val trainingData = data.ratings.map(r => {
      val like = if (r.rating > userMean(r.user)) 1.0 else 0D
      LabeledPoint(like, Vectors.dense(r.user.toInt, r.item.toInt))
    })

    val model = NaiveBayes.train(trainingData)

    new NBModel(
      sc.parallelize(userRatings.toSeq),
      sc.parallelize(userLikesAndNearstPearson._2.toSeq),//用户Pearson系数最近的N个用户
      sc.parallelize(userLikesAndNearstPearson._1.toSeq),//用户喜欢的N部电影
      model)
  }

  override def predict(model: NBModel, query: Query): PredictedResult = {
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


    val neurModel = model.navieBayesModel
    val filtedResult = pearsonResult.filter(re => {
      val v = Vectors.dense(query.user.toInt, re._1.toInt)
      neurModel.predict(v) == 1.0
    })


    //排序取TopN
    val preResult = filtedResult.map(r => (r._1, r._2)).toList.sortBy(_._2).reverse.take(query.num).map(r => (r._1, r._2))

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum

    if(sum==0) return PredictedResult(Array.empty)

    val weight = 1.0
    val returnResult = pearsonResult.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    })

    //排序，返回结果
    PredictedResult(returnResult.toArray)
  }

  override def batchPredict(m: NBModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    val queryArray= qs.collect()

    val result = new ArrayBuffer[(Long, PredictedResult)]()

    for(r <-queryArray){
      logger.info(s"Index:${r._1},"+r._2)
      val pred=predict(m, r._2)
      result.append((r._1, pred))
      logger.info(pred)
    }
    logger.info(s"result的大小:${result.length}")
    qs.sparkContext.parallelize(result)
  }
}
