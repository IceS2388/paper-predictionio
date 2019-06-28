package org.example.recommendation

import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD


/**
  * Preparator处理数据，把数据传递给算法和模型。
  *
  * */
class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  /**简单把数据封装一下
    * 对输入的TrainingData执行任何必要的特性选择和数据处理任务
    * */
  override
  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    //默认知识简单复制一下，然后返回PreparedData类型。下一步是传给Algorithm的train方法
    //加入偏好系数

   /* val userPro= trainingData.ratings.map(r=>(r.user,(r.rating,1,-1.0,-1.0))).reduceByKey((r1,r2)=>{
      var max=r1._3
      if(max<0.0) max=r1._1

      if(max<r2._1) max=r2._1

      var min=r1._4
      if(min<0.0) min=r1._1

      if(min>r2._1) min=r2._1

      (r1._1+r2._1,r1._2+r2._2,max,min)
    })
      .map(re=>{
        val p= (re._2._1/re._2._2-re._2._4)/(re._2._3-re._2._4)
        (re._1,p)})*/

    //计算标准差和平均值
  val meanAndVariance= trainingData.ratings.map(r => (r.user,r.rating)).groupByKey().map(t2=>{
     val rdd= sc.parallelize(t2._2.map(r=>Vectors.dense(r)).toSeq)
     val summary= Statistics.colStats(rdd)
     (t2._1,(summary.mean.apply(0),summary.variance.apply(0)))
   })

    //转换到Map方便下一步使用
    var usersMap=Map[String,(Double,Double)]()
    meanAndVariance.map(r=>usersMap += r)

    //标准化用户的评分
    val newTrainingData= trainingData.ratings.map(r=>{
      val mean=usersMap.get(r.user).get._1
      val variance=usersMap.get(r.user).get._2
      val newRating= (r.rating-mean)/variance

      Rating(r.user,r.item,newRating)
    })

    new PreparedData(ratings = newTrainingData)
  }
}
/**
  * 一个可序列化的只包含ratings的RDD
  * */
class PreparedData(
  val ratings: RDD[Rating] //这是处理过后的评分矩阵
) extends Serializable
