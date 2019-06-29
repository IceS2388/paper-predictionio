package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{EmptyEvaluationInfo, PDataSource, Params}
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(
  appName: String,
  evalParams: Option[DataSourceEvalParams]) extends Params
/**
  * DataSource从输入源读入数据，并转变成指定格式。
  * */
class DataSource(val dsp: DataSourceParams)  extends PDataSource[TrainingData,EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  def getRatings(sc: SparkContext): RDD[Rating] = {

    /**
      * PEventStore是一个object类型，提供访问PredictionIO Event Server收集的数据的方法。PEventStore.find(...)
      * PredictionIO automatically loads the parameters of datasource specified in MyRecommendation/engine.json,
      * including appName, to dsp.
      * PredictionIO自动加载从engine.json中设定的参数，包括appName到dsp中。
      * */
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("rate")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)



    val ratingsRDD: RDD[Rating] = eventsRDD.map { eventRecord =>
      val rating = try {
        val ratingValue: Double = eventRecord.event match {
          case "rate" => eventRecord.properties.get[Double]("rating")
          //case "buy" => 4.0 // map buy event to rating value of 4
          case _ => throw new Exception(s"Unexpected event ${eventRecord} is read.")
        }
        // entityId and targetEntityId is String
        Rating(eventRecord.entityId,
          eventRecord.targetEntityId.get,
          ratingValue)
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${eventRecord} to Rating. Exception: ${e}.")
          throw e
        }
      }
      rating
    }.cache()

    ratingsRDD
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {
    /**
      * 从Event Store(Event Server的数据仓库中)读取，选择数据，然后返回TrainningData
      * */
    new TrainingData(getRatings(sc))
  }

  /**
    * 从datastore中读取和选择数据，返回序列(training,validation)
    * */
  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {

    //检查评估参数
    require(!dsp.evalParams.isEmpty, "Must specify evalParams")

    //包含kFold和查询的数量
    val evalParams = dsp.evalParams.get

    val kFold = evalParams.kFold
    //Zips this RDD with generated unique Long ids.
    //生成(RDD[Rating],Long)类型的元组，Long是唯一的
    val ratings: RDD[(Rating, Long)] = getRatings(sc).zipWithUniqueId
    ratings.cache

    //分割数据
    (0 until kFold).map { idx => {
      //训练集
      val trainingRatings = ratings.filter(_._2 % kFold != idx).map(_._1)
      //测试集
      val testingRatings = ratings.filter(_._2 % kFold == idx).map(_._1)
      //测试集按照用户ID进行分组，便于验证。
      val testingUsers: RDD[(String, Iterable[Rating])] = testingRatings.groupBy(_.user)
      //返回类型
      (new TrainingData(trainingRatings),
        new EmptyEvaluationInfo(),
        testingUsers.map {
          case (user, ratings) => (Query(user, evalParams.queryNum), ActualResult(ratings.toArray))
        }
      )
    }}
  }
}
/**
  * 评分:
  * 用户ID
  * 物品ID
  * 评分
  * Spark MLlib's Rating类，只能使用Int类型的userID和itemID，为了灵活性，自定义一个String类型userID和itemID的Rating。
  * */
case class Rating(
  user: String,
  item: String,
  rating: Double
)
/**
  * TrainingData包含所有上面定义的Rating类型数据。
  * */
class TrainingData(
  val ratings: RDD[Rating]
) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}
