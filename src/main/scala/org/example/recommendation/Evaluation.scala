package org.example.recommendation

import org.apache.predictionio.controller._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 在本推荐引擎中，使用了2个维度的指标，来评价数据。
  * 用法:
  * $ pio eval org.example.recommendation.RecommendationEvaluation \
  * org.example.recommendation.EngineParamsList
  * 说明：
  * 本类只求推荐引擎对应的时长、精确度和召回率
  *
  * @param k 表示当前所在的测试集索引号
  **/
case class VerifiedResult (val precision:Double,val recall:Double,val f1:Double){

}

case class Recommendation
  extends Metric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult,VerifiedResult]{

  override
  def calculate(sc: SparkContext, evalDataSet: Seq[(EmptyEvaluationInfo, RDD[(Query, PredictedResult, ActualResult)])]): VerifiedResult = {
    /**
      *               P(Predicted)      N(Predicted)
      *
      * P(Actual)     True Positive      False Negative
      *
      * N(Actual)     False Positive     True Negative
      *
      * Precision = TP / (TP + FP)      分母为预测时推荐的记录条数
      *
      * Recall = TP / (TP + FN)         分母为测试时该用户拥有的测试记录条数
      *
      * F1 = 2TP / (2TP + FP + FN)
      * */
     val finalV= evalDataSet.map(r=>{
      //r._2 RDD[(Query, PredictedResult, ActualResult)])] 这是每条参数的对应每次的预测结果

      val each= r._2.map(p=>{
        //这里是每一条结果
        //p._1.user
        //p._1.num
        //p._2.itemScores
        //p._3.ratings
        //改用户的测试物品
        val actuallyItems=p._3.ratings.map(ar=>ar.item)
        val predictedItems=p._2.itemScores.map(ir=>ir.item)
        if (predictedItems.size==0){
          //返回每一个用户ID的验证结果
          VerifiedResult(0,0,0)
        }else{
          //命中的数量TP
          val hit=actuallyItems.toSet.intersect(predictedItems.toSet).size
          //Precision = TP / (TP + FP)
          val precision = hit * 1.0 / predictedItems.size
          //Recall = TP / (TP + FN)
          val recall = hit * 1.0 / actuallyItems.size
          //F1 = 2TP / (2TP + FP + FN)
          val f1=2*hit/(predictedItems.size+actuallyItems.size)
          //返回每一个用户ID的验证结果
          VerifiedResult(precision,recall,f1)
        }
      })

      val count=each.count()
      val t: VerifiedResult =each.reduce((v1, v2)=>{
        VerifiedResult(v1.precision+v2.precision,v1.recall+v2.recall,v1.f1+v2.f1)
      })

      //返回这个参数下：所有验证结果的平均值
      VerifiedResult(t.precision/count,t.recall/count,t.f1/count)
    })

    val fCount=finalV.size
    val tTop=finalV.reduce((v1,v2)=>{
      VerifiedResult(v1.precision+v2.precision,v1.recall+v2.recall,v1.f1+v2.f1)
    })
    VerifiedResult(tTop.precision/fCount,tTop.recall/fCount,tTop.f1/fCount)
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
      metric = Recommendation(),
      otherMetrics = Seq(

      ), "evalResult"))
}

object EngineParamsList extends EngineParamsGenerator {
  //EngineParamsList用于定义评估的参数列表

  //首先，定义基本的引擎参数。它的appName指定了读取的数据源，评估参数evalParams用于定义交叉验证。
  //DataSourceEvalParams:第一个10是分成10份，第二个是推荐的个数
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "MyApp1", evalParams = Some(DataSourceEvalParams(10, 20))))



  //然后，精确指定每个引擎的参数列表，同一个引擎可以有多个不同的测试参数。
  engineParamsList = Seq(
    baseEP.copy(algorithmParamsList = Seq(("als", ALSAlgorithmParams(10, 20, 0.01, Some(3L))))),
    baseEP.copy(algorithmParamsList = Seq(("prt", PRTAlgorithmParams(5, 20,20)))),
    baseEP.copy(algorithmParamsList = Seq(("prt", PRTAlgorithmParams(10, 20,20)))),
    baseEP.copy(algorithmParamsList = Seq(("prt", PRTAlgorithmParams(5, 20,40)))),
    baseEP.copy(algorithmParamsList = Seq(("prt", PRTAlgorithmParams(10, 20,40)))),

    baseEP.copy(algorithmParamsList = Seq(("mv", MViewAlgorithmParams(100)))),
    baseEP.copy(algorithmParamsList = Seq(("mv", MViewAlgorithmParams(200)))),
    baseEP.copy(algorithmParamsList = Seq(("mv", MViewAlgorithmParams(300)))),

    baseEP.copy(algorithmParamsList = Seq(("nb", NBAlgorithmParams(5, 20,20)))),
    baseEP.copy(algorithmParamsList = Seq(("nb", NBAlgorithmParams(10, 20,20)))),
    baseEP.copy(algorithmParamsList = Seq(("nb", NBAlgorithmParams(5, 20,40)))),
    baseEP.copy(algorithmParamsList = Seq(("nb", NBAlgorithmParams(10, 20,40))))
  )
}

