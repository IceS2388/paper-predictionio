package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author:IceS
  * Date:2019-07-23 14:09:25
  * Description:
  * 单纯的PearsonModel
  */
class PearsonModel(
                    val userMap: RDD[(String, Iterable[Rating])],
                    val userNearestPearson: RDD[(String, List[(String, Double)])],
                    val userLikesBeyondMean: RDD[(String, List[Rating])]
                  ) extends PersistentModel[PearsonAlgorithmParams]  {
  override def save(id: String, params: PearsonAlgorithmParams, sc: SparkContext): Boolean = {
    userMap.saveAsObjectFile(s"/tmp/P/$id/userMap")
    userNearestPearson.saveAsObjectFile(s"/tmp/P/$id/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/P/$id/userLikesBeyondMean")
    true
  }

  override def toString = {
    s"PearsonModel:{userMap.count:${userMap.count()}" +
      s",userNearestPearson.count:${userNearestPearson.count()}" +
      s",userLikesBeyondMean:${userLikesBeyondMean.count()}"
  }
}

object PearsonModel extends PersistentModelLoader[PearsonAlgorithmParams, PearsonModel] {
  override def apply(id: String, params: PearsonAlgorithmParams, sc: Option[SparkContext]): PearsonModel = {
    new PearsonModel(
      userMap = sc.get.objectFile[(String, Iterable[Rating])](s"/tmp/P/$id/userMap"),
      userNearestPearson = sc.get.objectFile[(String, List[(String, Double)])](s"/tmp/P/$id/userNearestPearson"),
      userLikesBeyondMean = sc.get.objectFile[(String, List[Rating])](s"/tmp/P/$id/userLikesBeyondMean")

    )
  }
}