package paper.model


import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.example.recommendation.Rating
import paper.algorithm.PUSAlgorithmParams


/**
  * Pearson用户相似度模板
  **/
class PUSModel(
                val userMap: Map[String, Iterable[Rating]],
                val userNearestPearson: Seq[(String, List[(String, Double)])],
                val userLikesBeyondMean: Seq[(String, List[Rating])]
              ) extends PersistentModel[PUSAlgorithmParams] {
  override def save(id: String, params: PUSAlgorithmParams, sc: SparkContext): Boolean = {

    sc.parallelize(userMap.toList).saveAsObjectFile(s"/tmp/${id}/userMap")
    sc.parallelize(userNearestPearson).saveAsObjectFile(s"/tmp/${id}/userNearestPearson")
    sc.parallelize(userLikesBeyondMean).saveAsObjectFile(s"/tmp/${id}/userLikesBeyondMean")
    true
  }

  override def toString: String = {
    s"userMap: mutable.Map[String, Iterable[Rating]]->${userMap.size}" +
      s"userNearestPearson: Seq[(String, List[(String, Double)])]->${userNearestPearson.size}" +
      s"userLikesBeyondMean: RDD[(String, List[Rating])]->${userLikesBeyondMean.size}"
  }
}

object PUSModel extends PersistentModelLoader[PUSAlgorithmParams, PUSModel] {
  override def apply(id: String, params: PUSAlgorithmParams, sc: Option[SparkContext]): PUSModel = {
    new PUSModel(
      userMap = sc.get.objectFile[Map[String, Iterable[Rating]]](s"/tmp/${id}/userMap").first(),
      userNearestPearson = sc.get.objectFile[Seq[(String, List[(String, Double)])]](s"/tmp/${id}/userNearestPearson").first(),
      userLikesBeyondMean = sc.get.objectFile[Seq[(String, List[Rating])]](s"/tmp/${id}/userLikesBeyondMean").first()
    )
  }
}
