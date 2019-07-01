package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

    //用户的观影记录
    val userGroup = data.ratings.groupBy(r => r.item)

    var initalSize: RDD[(String, Int)] = userGroup.map(r => (r._1, r._2.size))

    while (initalSize.count() > 2 * ap.maxItems) {
      //1.求平均值
      val sum = initalSize.map(_._2).sum()
      val mean = sum / initalSize.count()
      //2.筛选
      initalSize = initalSize.filter(r => r._2 > mean)
    }

    val mostView: Array[(String, Int)] = initalSize.sortBy(_._2).collect().reverse.take(ap.maxItems)

    val userOwned = userGroup.map(r => {
      val items = r._2.map(r2 => r2.item)
      (r._1, items)
    })
    //logger.info(s"userOwned:${userOwned.count()}")
    //logger.info(s"mostView:${mostView.size}")
    new MViewModel(userOwned, sc.parallelize(mostView))
  }

  override def predict(model: MViewModel, query: Query): PredictedResult = {
    val sawMovie = model.userMap.collectAsMap()

    //该用户有看过
    val itemMap = sawMovie.get(query.user)
    val result = model.mostView.filter(r => {
      !itemMap.contains(r._1)
    }).take(query.num).map(r => {
      new ItemScore(r._1, r._2)
    })

    //logger.info(s"result:${result.length}")

    PredictedResult(result)
  }
}
