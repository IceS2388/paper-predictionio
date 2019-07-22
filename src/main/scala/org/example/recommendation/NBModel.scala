package org.example.recommendation

import java.io.File

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD


/**
  * Author:IceS
  * Date:2019-07-19 21:12:02
  * Description:
  * 神经网络包装模型。
  */
class NBModel(
               val userMap: RDD[(String, Iterable[Rating])],
               val userNearestPearson: RDD[(String, List[(String, Double)])],
               val userLikesBeyondMean: RDD[(String, List[Rating])],
               val navieBayesModel: NaiveBayesModel
             ) extends PersistentModel[NBAlgorithmParams] {
  override def save(id: String, params: NBAlgorithmParams, sc: SparkContext): Boolean = {
    userMap.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userMap")
    userNearestPearson.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userLikesBeyondMean")
    navieBayesModel.save(sc, s"/tmp/${this.getClass.getName}/$id/navieBayesModel")
    true
  }

  override def toString = {
    s"NeurModel:{userMap.count:${userMap.count()}" +
      s",userNearestPearson.count:${userNearestPearson.count()}" +
      s",userLikesBeyondMean:${userLikesBeyondMean.count()}" +
      s",model:${navieBayesModel.toString}"
  }
}

object NBModel extends PersistentModelLoader[NBAlgorithmParams, NBModel] {
  override def apply(id: String, params: NBAlgorithmParams, sc: Option[SparkContext]): NBModel = {
    new NBModel(
      userMap = sc.get.objectFile[(String, Iterable[Rating])](s"/tmp/${this.getClass.getName}/$id/userMap"),
      userNearestPearson = sc.get.objectFile[(String, List[(String, Double)])](s"/tmp/${this.getClass.getName}/$id/userNearestPearson"),
      userLikesBeyondMean = sc.get.objectFile[(String, List[Rating])](s"/tmp/${this.getClass.getName}/$id/userLikesBeyondMean"),
      NaiveBayesModel.load(sc.get, s"/tmp/${this.getClass.getName}/$id/navieBayesModel")

    )
  }
}