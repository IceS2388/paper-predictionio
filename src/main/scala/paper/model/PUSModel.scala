package paper.model


import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.example.recommendation.Rating
import paper.algorithm.PUSAlgorithmParams

import scala.collection.mutable


/**
  * Pearson用户相似度模板
  * */
class PUSModel(
  val userMap: mutable.Map[String, Iterable[Rating]],
  val userNearestPearson: Seq[(String, List[(String, Double)])],
  val userLikesBeyondMean: RDD[(String, List[Rating])]
   ) extends PersistentModel[PUSAlgorithmParams]{
  override def save(id: String, params: PUSAlgorithmParams, sc: SparkContext): Boolean = {

    sc.parallelize(userMap.toList).saveAsObjectFile(s"/tmp/${id}/userMap")
    sc.parallelize(userNearestPearson.toList).saveAsObjectFile(s"/tmp/${id}/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/${id}/userLikesBeyondMean")
    true
  }

  override def toString: String = {
    s"userMap: mutable.Map[String, Iterable[Rating]]->${userMap.size}"+
    s"userNearestPearson: Seq[(String, List[(String, Double)])]->${userNearestPearson.size}"+
    s"userLikesBeyondMean: RDD[(String, List[Rating])]->${userLikesBeyondMean.take(2).toString}"
  }
}
object PUSModel extends PersistentModelLoader[PUSAlgorithmParams,PUSModel]{
  override def apply(id: String, params: PUSAlgorithmParams, sc: Option[SparkContext]): PUSModel = {
    new PUSModel(
      userMap=sc.get.objectFile[mutable.Map[String, Iterable[Rating]]](s"/tmp/${id}/userMap").first(),
      userNearestPearson=sc.get.objectFile[Seq[(String, List[(String, Double)])]](s"/tmp/${id}/userNearestPearson").first(),
      userLikesBeyondMean = sc.get.objectFile(s"/tmp/${id}/userLikesBeyondMean")
    )
  }
}
