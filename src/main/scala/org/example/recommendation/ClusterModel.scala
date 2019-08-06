package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author:IceS
  * Date:2019-08-05 18:22:47
  * Description:
  * 基于聚类的模型
  */
class ClusterModel(
                    val userLikedRDD: RDD[(String, Seq[Rating])],
                    val nearestUserRDD: RDD[(String, Double)]
                  ) extends PersistentModel[ClusterAlgorithmParams] with Serializable{
  override def save(id: String, params: ClusterAlgorithmParams, sc: SparkContext): Boolean = {
    userLikedRDD.saveAsObjectFile(s"/tmp/C/$id/userLikedRDD")
    nearestUserRDD.saveAsObjectFile(s"/tmp/C/$id/nearestUserRDD")
    true
  }

  override def toString: String = {
    s"ClusterModel:{userLikedRDD.count:${userLikedRDD.count()}" +
      s",nearestUserRDD.count:${nearestUserRDD.count()}"
  }
}
object ClusterModel extends PersistentModelLoader[ClusterAlgorithmParams, ClusterModel] {
  override def apply(id: String, params: ClusterAlgorithmParams, sc: Option[SparkContext]): ClusterModel = {
    new ClusterModel(
      sc.get.objectFile[(String, Seq[Rating])](s"/tmp/C/$id/userLikedRDD"),
      sc.get.objectFile[(String, Double)](s"/tmp/C/$id/nearestUserRDD")
    )
  }
}