package org.example.recommendation

import java.io.File

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

/**
  * Author:IceS
  * Date:2019-07-19 21:12:02
  * Description:
  * 神经网络包装模型。
  */
class NeurModel(
                 val userMap: RDD[(String, Iterable[Rating])],
                 val userNearestPearson: RDD[(String, List[(String, Double)])],
                 val userLikesBeyondMean: RDD[(String, List[Rating])],
                 val neurModel: MultiLayerNetwork
               ) extends PersistentModel[NeurAlgorithmParams] {
  override def save(id: String, params: NeurAlgorithmParams, sc: SparkContext): Boolean = {
    userMap.saveAsObjectFile(s"/tmp/PN/$id/userMap")
    userNearestPearson.saveAsObjectFile(s"/tmp/PN/$id/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/PN/$id/userLikesBeyondMean")
    neurModel.save(new File(s"/tmp/PN/$id/neurModel"))
    true
  }

  override def toString = {
    s"NeurModel:{userMap.count:${userMap.count()}" +
      s",userNearestPearson.count:${userNearestPearson.count()}" +
      s",userLikesBeyondMean:${userLikesBeyondMean.count()}" +
      s",model:${neurModel.toString}"
  }
}

object NeurModel extends PersistentModelLoader[NeurAlgorithmParams, NeurModel] {
  override def apply(id: String, params: NeurAlgorithmParams, sc: Option[SparkContext]): NeurModel = {
    new NeurModel(
      userMap = sc.get.objectFile[(String, Iterable[Rating])](s"/tmp/PN/$id/userMap"),
      userNearestPearson = sc.get.objectFile[(String, List[(String, Double)])](s"/tmp/PN/$id/userNearestPearson"),
      userLikesBeyondMean = sc.get.objectFile[(String, List[Rating])](s"/tmp/PN/$id/userLikesBeyondMean"),
      MultiLayerNetwork.load(new File(s"/tmp/PN/$id/neurModel"), true)
    )
  }
}