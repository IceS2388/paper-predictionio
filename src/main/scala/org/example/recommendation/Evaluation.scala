package org.example.recommendation

import org.apache.predictionio.controller._


/**
  * 在本推荐引擎中，使用了2个维度的指标，来评价数据。
  **/

/**
  * 用法:
  * $ pio eval org.example.recommendation.RecommendationEvaluation \
  * org.example.recommendation.EngineParamsList
  * 说明：
  * 本类只求推荐引擎对应的时长、精确度和召回率
  *
  * @param k 表示当前所在的测试集索引号
  **/
case class PrecisionAtK()
  extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {

    //1.获取所有按照用户ID分组的评分记录
    //a.ratings//是某个用户评估的Rating。
    val items = a.ratings.map(r => r.item).toSet //当前用户评分的item集合

    if (items.size == 0) {
      return Some(0)
    }

    //2.获取推荐引擎所推荐的结果
    val recommendItems = p.itemScores.map(r => r.item).toSet

    //3.计算指标
    //命中的个数
    val hit = items.intersect(recommendItems).size
    val precision = hit * 1.0 / recommendItems.size

    Some(precision)
  }
}

/**
  * 计算召回率
  **/
case class RecallAtK() extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    //1.获取所有按照用户ID分组的评分记录
    val items = a.ratings.map(r => r.item).toSet //当前用户评分的item集合

    if (items.size == 0) {
      return Some(0)
    }

    //2.获取推荐引擎所推荐的结果
    val recommendItems = p.itemScores.map(r => r.item).toSet

    //3.计算指标
    val hit = items.intersect(recommendItems).size
    val recall = hit * 1.0 / items.size
    Some(recall)
  }
}


/**
  * 评估指标用数字分数量化预测准确度。它可用于比较算法或算法参数设置。命令行参数指定
  **/
object RecommendationEvaluation extends Evaluation {
  engineEvaluator = (
    RecommendationEngine(),
    //度量评估
    MetricEvaluator(
      //设置评估参数
      metric = PrecisionAtK(),
      otherMetrics = Seq(
        RecallAtK()
      ), "evalResult"))
}

object EngineParamsList extends EngineParamsGenerator {
  // Define list of EngineParams used in Evaluation

  // First, we define the base engine params. It specifies the appName from which
  // the data is read, and a evalParams parameter is used to define the
  // cross-validation.
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "MyApp1", evalParams = Some(DataSourceEvalParams(10, 10))))


  // Second, we specify the engine params list by explicitly listing all
  // algorithm parameters. In this case, we evaluate 3 engine params, each with
  // a different algorithm params value.
  engineParamsList = Seq(
    baseEP.copy(algorithmParamsList = Seq(("als", ALSAlgorithmParams(10, 20, 0.01, Some(3L))))),
    baseEP.copy(algorithmParamsList = Seq(("pus", PUSAlgorithmParams(10, 50)))),
    baseEP.copy(algorithmParamsList = Seq(("mv", MViewAlgorithmParams(300))))
  )
}

