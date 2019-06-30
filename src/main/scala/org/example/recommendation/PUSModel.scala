package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  * Pearson用户相似度模板
  **/
class PUSModel(
                val userMap: RDD[(String, Iterable[Rating])],
                val userNearestPearson: RDD[(String, List[(String, Double)])],
                val userLikesBeyondMean: RDD[(String, List[Rating])]
              ) extends PersistentModel[PUSAlgorithmParams] {
  override def save(id: String, params: PUSAlgorithmParams, sc: SparkContext): Boolean = {

    userMap.saveAsObjectFile(s"/tmp/${id}/userMap")
    userNearestPearson.saveAsObjectFile(s"/tmp/${id}/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/${id}/userLikesBeyondMean")
    true
  }

  override def toString: String = {
    s"userMap: mutable.Map[String, Iterable[Rating]]->${userMap.count}" +
      s"userNearestPearson: Seq[(String, List[(String, Double)])]->${userNearestPearson.count}" +
      s"userLikesBeyondMean: RDD[(String, List[Rating])]->${userLikesBeyondMean.count}"
  }
}

object PUSModel extends PersistentModelLoader[PUSAlgorithmParams, PUSModel] {
  override def apply(id: String, params: PUSAlgorithmParams, sc: Option[SparkContext]): PUSModel = {
    new PUSModel(
      userMap = sc.get.objectFile[(String, Iterable[Rating])](s"/tmp/${id}/userMap"),
      userNearestPearson = sc.get.objectFile[(String, List[(String, Double)])](s"/tmp/${id}/userNearestPearson"),
      userLikesBeyondMean = sc.get.objectFile[(String, List[Rating])](s"/tmp/${id}/userLikesBeyondMean")
    )
  }
}
