package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD


/**
  * Pearson与随机森林的模板。
  * @param userMap 用户对应的评分列表。
  * @param userNearestPearson 与用户Pearson系数最大的前N个用户。
  * @param userLikesBeyondMean 用户最喜欢的前N部电影，从看过的历史记录中筛选。
  * @param randomForestModel 随机森林模型
  **/
class PRTModel(
                val userMap: RDD[(String, Iterable[Rating])],
                val userNearestPearson: RDD[(String, List[(String, Double)])],
                val userLikesBeyondMean: RDD[(String, List[Rating])],
                val randomForestModel: RandomForestModel
              ) extends PersistentModel[PRTAlgorithmParams] {
  override def save(id: String, params: PRTAlgorithmParams, sc: SparkContext): Boolean = {

    userMap.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userMap")
    userNearestPearson.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userNearestPearson")
    userLikesBeyondMean.saveAsObjectFile(s"/tmp/${this.getClass.getName}/$id/userLikesBeyondMean")
    randomForestModel.save(sc,s"/tmp/${this.getClass.getName}/$id/randomForestModel")
    true
  }

  override def toString: String = {
    s"userMap: mutable.Map[String, Iterable[Rating]]->${userMap.count}" +
      s"userNearestPearson: Seq[(String, List[(String, Double)])]->${userNearestPearson.count}" +
      s"userLikesBeyondMean: RDD[(String, List[Rating])]->${userLikesBeyondMean.count}"
  }
}

object PRTModel extends PersistentModelLoader[PRTAlgorithmParams, PRTModel] {
  override def apply(id: String, params: PRTAlgorithmParams, sc: Option[SparkContext]): PRTModel = {
    new PRTModel(
      userMap = sc.get.objectFile[(String, Iterable[Rating])](s"/tmp/${this.getClass.getName}/$id/userMap"),
      userNearestPearson = sc.get.objectFile[(String, List[(String, Double)])](s"/tmp/${this.getClass.getName}/$id/userNearestPearson"),
      userLikesBeyondMean = sc.get.objectFile[(String, List[Rating])](s"/tmp/${this.getClass.getName}/$id/userLikesBeyondMean"),
      RandomForestModel.load(sc.get,s"/tmp/${this.getClass.getName}/$id/randomForestModel")
    )
  }
}
