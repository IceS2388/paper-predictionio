package org.example.recommendation

import org.apache.predictionio.controller.LServing
/**
  * Serving获取预测的查询结果。如果引擎有多重算法，Serving会将结果合并为一个。
  * 此外，可以在“服务”中添加特定于业务的逻辑，以进一步自定义最终返回的结果。
  *
  * An engine can train multiple models if you specify more than one Algorithm component in
  * 一个engine可以在Engine.scala文件中的object RecommendationEngine的伴生对象中指定多个模型。
  * */
class Serving
  extends LServing[Query, PredictedResult] {

  /**
    * 本方法处理预测的结果。可聚合多个预测模型的预测结果。这里返回的是最终的结果。PredictionIO会自动转换成JSON格式。
    * */
  override
  def serve(query: Query,
    predictedResults: Seq[PredictedResult]): PredictedResult = {
    predictedResults.head
  }
}
