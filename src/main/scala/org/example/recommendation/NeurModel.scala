package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @Author:Administrator
  * @Date:2019-07-19 21:12:02
  * @Description:
  * 神经网络包装模型。
  */
class NeurModel(
                 val userMap: RDD[(String, Iterable[Rating])],
                 val userNearestPearson: RDD[(String, List[(String, Double)])],
                 val userLikesBeyondMean: RDD[(String, List[Rating])]
               ) extends PersistentModel[NeurAlgorithm] {
  override def save(id: String, params: NeurAlgorithm, sc: SparkContext): Boolean = ???

  override def toString = s"NeurModel()"
}
object NeurModel extends PersistentModelLoader[NeurAlgorithm, NeurModel] {
  override def apply(id: String, params: NeurAlgorithm, sc: Option[SparkContext]): NeurModel = ???
}