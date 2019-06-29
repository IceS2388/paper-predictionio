package paper.model


import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.example.recommendation.Rating
import paper.algorithm.PearsonUserSimilarityAlgorithmParams

import scala.collection.mutable


/**
  * Pearson用户相似度模板
  * */
class PearsonUserCorrelationSimilarityModel(
   val rank: Int,
   userMap: mutable.Map[String, Iterable[Rating]],
   userNearestPearson: Seq[(String, List[(String, Double)])]
   ) extends PersistentModel[PearsonUserSimilarityAlgorithmParams]{
  override def save(id: String, params: PearsonUserSimilarityAlgorithmParams, sc: SparkContext): Boolean = {
    sc.parallelize(Seq(rank)).saveAsObjectFile(s"/tmp/${id}/rank")
    sc.parallelize(userMap.toList).saveAsObjectFile(s"/tmp/${id}/userMap")
    sc.parallelize(userNearestPearson.toList).saveAsObjectFile(s"/tmp/${id}/userNearestPearson")
    true
  }

  override def toString: String = {
    s"rank:${rank}"+
    s"userMap: mutable.Map[String, Iterable[Rating]]->${userMap.size}"+
    s"userNearestPearson: Seq[(String, List[(String, Double)])]->${userNearestPearson.size}"
  }
}
object PearsonUserCorrelationSimilarityModel extends PersistentModelLoader[PearsonUserSimilarityAlgorithmParams,PearsonUserCorrelationSimilarityModel]{
  override def apply(id: String, params: PearsonUserSimilarityAlgorithmParams, sc: Option[SparkContext]): PearsonUserCorrelationSimilarityModel = {
    new PearsonUserCorrelationSimilarityModel(
      rank = sc.get.objectFile[Int](s"/tmp/${id}/rank").first(),
      userMap=sc.get.objectFile[mutable.Map[String, Iterable[Rating]]](s"/tmp/${id}/userMap").first(),
      userNearestPearson=sc.get.objectFile[Seq[(String, List[(String, Double)])]](s"/tmp/${id}/userNearestPearson").first()
    )
  }
}
