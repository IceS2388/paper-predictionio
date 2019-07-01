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

    //1.Als推荐
    val alsResult=predictedResults.head
    var alsSum=0D
    alsResult.itemScores.foreach(r=>{
      alsSum+=r.score
    })
    //归一化后存储
    alsResult.itemScores.foreach(r=>{
     result.put(r.item,r.score/alsSum)
    })

    //2.Pearson推荐
    val pearsonResult=predictedResults.take(2).last
    var pearsonSum=0D
    pearsonResult.itemScores.foreach(r=>{
      pearsonSum+=r.score
    })
    val pearsonWeight=1.5 //这里设置pearson的系数权重
    //归一化后存储
    pearsonResult.itemScores.foreach(r=>{

      if(!result.contains(r.item)){
        result.put(r.item,pearsonWeight*r.score/pearsonSum)
      }else{
        //其他算法提供的预测结果
        val oldScore:Double =result.get(r.item).get
        result.put(r.item,pearsonWeight*r.score/pearsonSum+oldScore)
      }
    })

    //3.MV推荐
    val mvResult=predictedResults.take(3).last
    logger.info(s"mvResult.size:${mvResult.itemScores.length}")
    var mvSum=0D
    mvResult.itemScores.foreach(r=>{
      mvSum+=r.score
    })
    val mvWeight=1.5 //这里设置MV的系数权重
    //归一化后存储
    mvResult.itemScores.foreach(r=>{
      if(!result.contains(r.item)){
        result.put(r.item,mvWeight*r.score/mvSum)
      }else{
        //其他算法提供的预测结果
        val oldScore:Double =result.get(r.item).get
        result.put(r.item,mvWeight*r.score/mvSum+oldScore)
      }
    })



    logger.info("ALS算法")
    predictedResults.head.itemScores.foreach(r=>{
      logger.info(s"item:${r.item},score:${r.score/alsSum}")
    })
    logger.info("Pearson算法")
    predictedResults.take(2).last.itemScores.foreach(r=>{
      logger.info(s"item:${r.item},1.5*score:${pearsonWeight*r.score/pearsonSum}")
    })
    logger.info("MV算法")
    predictedResults.take(3).last.itemScores.foreach(r=>{
      logger.info(s"item:${r.item},1.5*score:${mvWeight*r.score/mvSum}")
    })

    PredictedResult(result.map(r=>new ItemScore(r._1,r._2)).toArray.sortBy(_.score).reverse.take(query.num))
  }
}
