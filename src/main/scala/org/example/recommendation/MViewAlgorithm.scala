package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * @param maxItems 获取访问次数最多的前maxItems电影。
  **/
case class MViewAlgorithmParams(maxItems: Int) extends Params

/**
  * 功能：
  * 实现推荐访问最多的电影。
  **/
class MViewAlgorithm(val ap: MViewAlgorithmParams) extends PAlgorithm[PreparedData, MViewModel, Query, PredictedResult] {
  @transient lazy val logger = Logger[this.type]

  override def train(sc: SparkContext, data: PreparedData): MViewModel = {

    //电影分组
    val itemGroup = data.ratings.groupBy(r => r.item)
    //(电影ID，观影次数)
    var initalSize: RDD[(String, Int)] = itemGroup.map(r => (r._1, r._2.size))

    //调试信息
    /*initalSize.foreach(r=>{
      logger.info(s"itemid:${r._1},count:${r._2}")
    })*/


    //logger.info(s"电影的总数：${initalSize.count()}")
    while (initalSize.count() > 2 * ap.maxItems) {

      //1.求平均值
      val sum = initalSize.map(_._2).sum()
      val mean = sum / initalSize.count()
      //2.筛选
      initalSize = initalSize.filter(r => r._2 > mean)
      //logger.info(s"筛选后剩余的电影的总数：${initalSize.count()}")
    }

    val mostView: Array[(String, Int)] = initalSize.sortBy(_._2).collect().reverse.take(ap.maxItems)

    //用户看过的
    val userOwned =data.ratings.groupBy(_.user).map(r => {
      val items = r._2.map(r2 => r2.item)
      (r._1, items)
    })
    new MViewModel(userOwned, sc.parallelize(mostView))
  }

  override def predict(model: MViewModel, query: Query): PredictedResult = {
    val sawMovie = model.userMap.collectAsMap()

    //该用户已经看过的电影
    val itemMap = sawMovie.get(query.user)


    //调试信息
   /* logger.info(s"筛选前的大小：${model.mostView.count()}")
    val myCount=model.mostView.filter(r => {
      val re= itemMap.contains(r._1)
      logger.info(s"${r._1}是否包含在用户的观看列表：${re}")
      re
    }).count()
    logger.info(s"用户观看列表的大小：${itemMap.get.size}；包含在最喜欢看列表中的个数为：${myCount}")*/



    val result = model.mostView.filter(r => {
      !itemMap.contains(r._1)
    }).take(query.num).map(r => {
      (r._1, r._2)
    })
    //logger.info(s"筛选后的大小：${result.size}")


    //实现归一化
    val sum= result.map(r=>r._2).sum
    if(sum==0) return PredictedResult(Array.empty)
    val weight=1.0
    val returnResult=result.map(r=>{
       ItemScore(r._1,r._2/sum*weight)
    })

    PredictedResult(returnResult)
  }

  override def batchPredict(m: MViewModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    val spark = qs.sparkContext
    val result = new ArrayBuffer[(Long, PredictedResult)]()

    qs.foreach(r => {
      //r._1
      logger.info(s"Index:${r._1}")
      result.append((r._1, predict(m, r._2)))
    })
    spark.parallelize(result)
  }
}
