package org.example.recommendation

import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.controller.OptionAverageMetric
import org.apache.predictionio.controller.AverageMetric
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.MetricEvaluator

// Usage:
// $ pio eval org.example.recommendation.RecommendationEvaluation \
//   org.example.recommendation.EngineParamsList
/**
  * 用法:
  * $ pio eval org.example.recommendation.RecommendationEvaluation \
  * org.example.recommendation.EngineParamsList
  *
  * */
case class PrecisionAtK(k: Int, ratingThreshold: Double = 2.0)
    extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {

  require(k > 0, "k must be greater than 0")
//打印消息：Precision的位数为k，门阀值：ratingThreshold
  override def header = s"Precision@K (k=$k, threshold=$ratingThreshold)"

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {

    //筛选用户对物品的评分小于最低值的物品集合
    val positives: Set[String] = a.ratings.filter(_.rating >= ratingThreshold).map(_.item).toSet

    // If there is no positive results, Precision is undefined. We don't consider this case in the
    // metrics, hence we return None.
    if (positives.size == 0) {
      None
    } else {
      //p是预测的结果,且排好序。
      //筛选：推荐的物品必须在物品集合中
      val tpCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size
      //计算的是命中率？点击率？
      Some(tpCount.toDouble / math.min(k, positives.size))
    }
  }
}

case class PositiveCount(ratingThreshold: Double = 2.0)
    extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  override def header = s"PositiveCount (threshold=$ratingThreshold)"

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Double = {
    //计算用户对物品的评分大于阈值的个数。
    a.ratings.filter(_.rating >= ratingThreshold).size
  }
}
/**
  * 评估指标用数字分数量化预测准确度。它可用于比较算法或算法参数设置。命令行参数指定
  * */
object RecommendationEvaluation extends Evaluation {
  engineEvaluator = (
    RecommendationEngine(),
    //矩阵计算器
    MetricEvaluator(
      metric = PrecisionAtK(k = 10, ratingThreshold = 4.0),
      otherMetrics = Seq(
        PositiveCount(ratingThreshold = 4.0),
        PrecisionAtK(k = 10, ratingThreshold = 2.0),
        PositiveCount(ratingThreshold = 2.0),
        PrecisionAtK(k = 10, ratingThreshold = 1.0),
        PositiveCount(ratingThreshold = 1.0)
      )))
}


object ComprehensiveRecommendationEvaluation extends Evaluation {
  val ratingThresholds = Seq(0.0, 2.0, 4.0)
  val ks = Seq(1, 3, 10)

  engineEvaluator = (
    RecommendationEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 3, ratingThreshold = 2.0),
      otherMetrics = (
        (for (r <- ratingThresholds) yield PositiveCount(ratingThreshold = r)) ++
        (for (r <- ratingThresholds; k <- ks) yield PrecisionAtK(k = k, ratingThreshold = r))
      )))
}


trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "INVALID_APP_NAME",
      evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 10))))
}

object EngineParamsList extends BaseEngineParamsList {
  engineParamsList = for(
    rank <- Seq(5, 10, 20);
    numIterations <- Seq(1, 5, 10))
    yield baseEP.copy(
      algorithmParamsList = Seq(
        ("als", ALSAlgorithmParams(rank, numIterations, 0.01, Some(3)))))
}
