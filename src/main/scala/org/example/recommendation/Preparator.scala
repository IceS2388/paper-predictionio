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

    //导入Spark SQL 过滤观看条数少于20条的用户
    val lowFreqencyUser = trainingData.ratings.mapPartitions(p=>{
      p.map(r=>(r.user,1))
    }).reduceByKey(_+_).filter(_._2<=20).collectAsMap()

    new PreparedData(ratings = trainingData.ratings
      //.filter(r=>(!lowFreqencyUser.contains(r.user)))
    )
  }
}

/**
  * 一个可序列化的只包含ratings的RDD
  **/
class PreparedData(
                    val ratings: RDD[Rating] //这是处理过后的评分矩阵
                  ) extends Serializable
