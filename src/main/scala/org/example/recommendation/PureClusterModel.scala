package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author:IceS
  * Date:2019-08-07 13:48:21
  * Description:
  * NONE
  */
class PureClusterModel (
                         val userBelongClusterIndex: RDD[(String, Int)],
                         val clusterItems: RDD[(String, Double)]
                       ) extends PersistentModel[PureClusterAlgorithmParams] with Serializable{
  override def save(id: String, params: PureClusterAlgorithmParams, sc: SparkContext): Boolean = {
    userBelongClusterIndex.saveAsObjectFile(s"/tmp/PC/$id/userBelongClusterIndex")
    clusterItems.saveAsObjectFile(s"/tmp/PC/$id/clusterItems")
    true
  }

  override def toString: String = {
    s"PureClusterModel:{userBelongClusterIndex.count():${userBelongClusterIndex.count()}" +
      s",clusterItems:${clusterItems.count()}"
  }
}
object PureClusterModel extends PersistentModelLoader[PureClusterAlgorithmParams, PureClusterModel] {
  override def apply(id: String, params: PureClusterAlgorithmParams, sc: Option[SparkContext]): PureClusterModel = {
    new PureClusterModel(
      sc.get.objectFile[(String, Int)](s"/tmp/PC/$id/userBelongClusterIndex"),
      sc.get.objectFile[(String, Double)](s"/tmp/PC/$id/clusterItems")
    )
  }
}