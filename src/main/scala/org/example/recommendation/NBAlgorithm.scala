package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

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
    val (userLikes,nearstPearson) = new SimilarityFactor(ap.pearsonThreashold, ap.numNearestUsers,ap.numUserLikeMovies).getNearstUsers(userRatings)

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
      sc.parallelize(nearstPearson.toSeq),//用户Pearson系数最近的N个用户
      sc.parallelize(userLikes.toSeq),//用户喜欢的N部电影
      model)
  }

  override def predict(model: NBModel, query: Query): PredictedResult = {

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


    val neurModel = model.navieBayesModel
    val filtedResult = result.filter(re => {
      val v = Vectors.dense(query.user.toInt, re._1.toInt)
      neurModel.predict(v) == 1.0
    })


    //排序取TopN
    val preResult = filtedResult.map(r => (r._1, r._2)).sortBy(_._2,false).take(query.num)

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum
    if(sum==0) return PredictedResult(Array.empty)

    val weight = 1.0
    val returnResult = preResult.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    })

    //排序，返回结果
    PredictedResult(returnResult)
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
