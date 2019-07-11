package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext

class RFModel() extends PersistentModel[RFAlgorithmParams] {
  override def save(id: String, params: RFAlgorithmParams, sc: SparkContext): Boolean = ???
}
object RFModel extends PersistentModelLoader[RFAlgorithmParams, RFModel] {
  override def apply(id: String, params: RFAlgorithmParams, sc: Option[SparkContext]): RFModel = ???
}