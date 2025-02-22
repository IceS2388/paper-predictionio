package org.example.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}


/**
  * 用户ID和查询数量
  **/
case class Query(
  user: String,
  num: Int
)

/**
  * ItemScore的数组，最后返回给用户的结果
  **/
case class PredictedResult(
  itemScores: Array[ItemScore]
)


/**
  * 物品的ID和评分
  */
case class ItemScore(
  item: String,
  score: Double
){
  override def toString: String = {
    s"item:${item},score:${score}"
  }
}
/**
  * 为验证的用户评分，Rating类型的数组。
  * 用户ID
  * 物品ID
  * 评分
  **/
case class ActualResult(
  ratings: Array[Rating]
)

/**
  * 自定义实现的推荐引擎.
  * 使用DataSource，Preparator，ALSAlgorithm和Serving
  **/
object RecommendationEngine extends EngineFactory {
  def apply() = {
    /**
      *控制使用的推荐算法
      * */
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map(
        "nb" -> classOf[NBAlgorithm],
        "als" -> classOf[ALSAlgorithm],
        "prt" -> classOf[PRTAlgorithm],
        "mv" -> classOf[MViewAlgorithm]),
      classOf[Serving])
  }
}
