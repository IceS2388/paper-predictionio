package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.LServing

import scala.collection.mutable
/**
  * Serving获取预测的查询结果。如果引擎有多重算法，Serving会将结果合并为一个。
  * 此外，可以在“服务”中添加特定于业务的逻辑，以进一步自定义最终返回的结果。
  *
  * An engine can train multiple models if you specify more than one Algorithm component in
  * 一个engine可以在Engine.scala文件中的object RecommendationEngine的伴生对象中指定多个模型。
  * */
class Serving
  extends LServing[Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  /**
    * 本方法处理预测的结果。可聚合多个预测模型的预测结果。这里返回的是最终的结果。PredictionIO会自动转换成JSON格式。
    * */
  override
  def serve(query: Query,
    predictedResults: Seq[PredictedResult]): PredictedResult = {
    //predictedResults.head //als
    //predictedResults.size //方法的总数
    logger.info(s"总的算法引擎数量：${predictedResults.size}")

    logger.info("1.---原始评分---")
    //1.展示als的评分结果
    logger.info("ALS算法")
    var alsSum=0D
    predictedResults.head.itemScores.foreach(r=>{
      alsSum+=r.score
      logger.info(r)
    })
    logger.info("Pearson算法")
    var pearsonSum=0D
    predictedResults.take(2).last.itemScores.foreach(r=>{
      pearsonSum+=r.score
      logger.info(r)
    })



    /**
      * 思路：
      *   不同算法预测出来的评分肯定不统一，为了方便计算必须归一化。再进行具体的权重系数必须仔细考虑。
      * */
    val result=new mutable.HashMap[String,Double]()
    predictedResults.map(pr=>{

      //对不同模型的预测评分，进行归一化。
      var sum=0D;
      pr.itemScores.map(r=>{
        sum+= r.score
      })

      pr.itemScores.map(r=>{
        if(!result.contains(r.item)){
          result.put(r.item,r.score/sum)
        }else{
          //其他算法提供的预测结果
          val oldScore:Double =result.get(r.item).get
          result.put(r.item,r.score/sum+oldScore)
        }
      })

    })

    logger.info("2.---归一化评分---")
    //1.展示als的评分结果
    logger.info("ALS算法")

    predictedResults.head.itemScores.foreach(r=>{
      logger.info(s"item:${r.item},score:${r.score/alsSum}")
    })
    logger.info("Pearson算法")
    predictedResults.take(2).last.itemScores.foreach(r=>{
      logger.info(s"item:${r.item},score:${r.score/pearsonSum}")
    })

    PredictedResult(result.map(r=>new ItemScore(r._1,r._2)).toArray.sortBy(_.score).reverse.take(query.num))
  }
}
