package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.LServing

import scala.collection.mutable
/**
  * Serving获取预测的查询结果。如果引擎有多重算法，Serving会将结果合并为一个。
  * 此外，可以在“服务”中添加特定于业务的逻辑，以进一步自定义最终返回的结果。
  * An engine can train multiple models if you specify more than one Algorithm component in
  * 一个engine可以在Engine.scala文件中的object RecommendationEngine的伴生对象中指定多个模型。
  * */
class Serving
  extends LServing[Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  /**
    * 本方法处理预测的结果。可聚合多个预测模型的预测结果。这里返回的是最终的结果。PredictionIO会自动转换成JSON格式。
    * 思路：
    *   不同算法预测出来的评分肯定不统一，为了方便计算必须归一化。再进行具体的权重系数必须仔细考虑。
    * */
  override
  def serve(query: Query,
    predictedResults: Seq[PredictedResult]): PredictedResult = {

    logger.info(s"推荐结果集的个数：${predictedResults.size}")

    //存储最终结果的HashMap
    val result=new mutable.HashMap[String,Double]()

    predictedResults.map(pr=>{
      pr.itemScores.map(is=>{
        if(!result.contains(is.item)){
          result.put(is.item,is.score)
        }else{
          val oldScore=result.get(is.item).get
          result.update(is.item,oldScore+is.score)
        }
      })
    })


    PredictedResult(result.map(r=>new ItemScore(r._1,r._2)).toArray.sortBy(_.score).reverse.take(query.num))
  }
}
