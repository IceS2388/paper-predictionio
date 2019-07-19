package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext

/**
  * @Author:Administrator
  * @Date:2019-07-19 21:11:37
  * @Description:
  * NONE
  */

case class NeurAlgorithmParams(pearsonThreasholds: Int, topNLikes: Int) extends Params

class NeurAlgorithm(val ap: NeurAlgorithmParams) extends PAlgorithm[PreparedData, NeurModel, Query, PredictedResult] {

  @transient lazy val logger: Logger = Logger[this.type]
  override def train(sc: SparkContext, data: PreparedData): NeurModel = {
    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    //1.转换为HashMap,方便计算Pearson相似度,这是个昂贵的操作
    val userRatings: Map[String, Iterable[Rating]] = data.ratings.groupBy(r => r.user).collectAsMap().toMap

    //2.计算用户与用户之间Pearson系数，并返回用户观看过后喜欢的列表和pearson系数最大的前TopN个用户的列表
    val userLikesAndNearstPearson = new Pearson(ap.pearsonThreasholds, ap.topNLikes).getPearsonNearstUsers(userRatings)

    //3.开始神经网络


  }

  override def predict(model: NeurModel, query: Query): PredictedResult = ???
}
