package org.example.recommendation

import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  * Preparator处理数据，把数据传递给算法和模型。
  *
  **/
class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  /** 简单把数据封装一下
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
    //SparkContext和SparkSession 都不能作为函数的参数，函数的参数传递的时候会被序列化，而这两个参数都不能序列化
    /*val meanAndVariance= trainingData.ratings.map(r => (r.user,r.rating)).groupByKey().map(t2=>{
       val rdd= sc.parallelize(t2._2.map(r=>Vectors.dense(r)).toSeq)
       val summary= Statistics.colStats(rdd)
       (t2._1,(summary.mean.apply(0),summary.variance.apply(0)))
     })*/

    val meanAndVariance = trainingData.ratings.map(r => (r.user, r.rating)).groupByKey().map(t2 => {

      var sum = 0.0
      var count = 0

      //计算平均值
      t2._2.map(r => {
        sum += r
        count = count + 1
      })

      if (count == 0) {
        (t2._1, (-1.0, -1.0))
      } else if (count == 1) {
        (t2._1, (sum, sum))
      } else {

        val mean = sum / count
        //计算标准差
        sum = 0.0
        t2._2.map(r => {
          sum += Math.pow((r - mean), 2)
        })

        val variance = Math.pow(sum / count, 0.5)
        (t2._1, (mean, variance))
      }
    })
    //转换到Map方便下一步使用
    var usersMap = Map[String, (Double, Double)]()
    meanAndVariance.map(r => usersMap += r)

    //标准化用户的评分
    val newTrainingData = trainingData.ratings.map(r => {
     val t= usersMap.getOrElse(r.user,(-1.0,-1.0))
      val mean:Double = t._1
      val variance = t._2
      if (mean < 0 && variance < 0) {
        Rating(r.user, r.item, 0)
      } else {
        val newRating = (r.rating - mean) / variance
        Rating(r.user, r.item, newRating)
      }
    })

    new PreparedData(ratings = newTrainingData)
  }
}

/**
  * 一个可序列化的只包含ratings的RDD
  **/
class PreparedData(
                    val ratings: RDD[Rating] //这是处理过后的评分矩阵
                  ) extends Serializable
