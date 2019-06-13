package org.example.recommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
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
    new PreparedData(ratings = trainingData.ratings)
  }
}
/**
  * 一个可序列化的只包含ratings的RDD
  * */
class PreparedData(
  val ratings: RDD[Rating]
) extends Serializable
