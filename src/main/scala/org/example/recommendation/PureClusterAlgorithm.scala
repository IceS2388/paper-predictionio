package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.LEventStore
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
  * Author:IceS
  * Date:2019-08-07 13:46:28
  * Description:
  * 纯粹的聚类算法，对整个族的统一聚类。不包含每个用户的相似度
  */
case class PureClusterAlgorithmParams(
                                   appName: String,
                                   k: Int = 10,
                                   method:String="cosine",
                                   maxIterations: Int = 20,
                                   numNearestUsers: Int = 60,
                                   numUserLikeMovies: Int = 100) extends Params
class PureClusterAlgorithm(val ap: PureClusterAlgorithmParams) extends PAlgorithm[PreparedData, PureClusterModel, Query, PredictedResult] {
  @transient lazy val logger: Logger = Logger[this.type]

  override def train(sc: SparkContext, pd: PreparedData): PureClusterModel = {

    //1.对数据进行聚类准备
    val rowRDD = pd.ratings.map(r => Row(r.user.toInt, r.rating))

    val fields: Seq[StructField] = List(
      StructField("uid", IntegerType, nullable = false),
      StructField("rating", DoubleType, nullable = false)
    )
    val schema = StructType(fields)
    val sparkSession = SparkSession.builder().master(sc.master).appName(sc.appName).getOrCreate()

    val ubDF = sparkSession.createDataFrame(rowRDD, schema)
    ubDF.createOrReplaceTempView("ratings")

    //2.生成用户的评分向量
    val userVectorsDF = sparkSession.sql(
      """
        |SELECT uid,
        |COUNT(CASE WHEN rating=0.5 THEN 1 END) AS c1,
        |COUNT(CASE WHEN rating=1.0 THEN 1 END) AS c2,
        |COUNT(CASE WHEN rating=1.5 THEN 1 END) AS c3,
        |COUNT(CASE WHEN rating=2.0 THEN 1 END) AS c4,
        |COUNT(CASE WHEN rating=2.5 THEN 1 END) AS c5,
        |COUNT(CASE WHEN rating=3.0 THEN 1 END) AS c6,
        |COUNT(CASE WHEN rating=3.5 THEN 1 END) AS c7,
        |COUNT(CASE WHEN rating=4.0 THEN 1 END) AS c8,
        |COUNT(CASE WHEN rating=4.5 THEN 1 END) AS c9,
        |COUNT(CASE WHEN rating=5.0 THEN 1 END) AS c10
        |FROM ratings
        |GROUP BY uid
        |ORDER BY uid ASC
      """.stripMargin)
    userVectorsDF.createOrReplaceTempView("uv")
    //调试信息
    //userVectorsDF.printSchema()
    //userVectorsDF.show(10)

    val userVectorsRDD = userVectorsDF.rdd.map(r => {
      (r.get(0).toString,
        Vectors.dense(Array(
          r.get(1).toString.toDouble,
          r.get(2).toString.toDouble,
          r.get(3).toString.toDouble,
          r.get(4).toString.toDouble,
          r.get(5).toString.toDouble,
          r.get(6).toString.toDouble,
          r.get(7).toString.toDouble,
          r.get(8).toString.toDouble,
          r.get(9).toString.toDouble,
          r.get(10).toString.toDouble)))
    })
    val featuresRDD = userVectorsRDD.map(_._2)
    //调试信息
    //logger.info(s"featuresRDD.count:${featuresRDD.count()}")

    logger.info("正在对用户评分向量进行聚类，需要些时间...")
    //3.准备聚类
    // TODO 2分聚类效果不太好k，均匀或基于密度比较合适
    val bkm = new BisectingKMeans().setK(ap.k).setMaxIterations(ap.maxIterations)
    val model = bkm.run(featuresRDD)

    //调试信息
    model.clusterCenters.foreach(println)


    //4.聚类用户评分向量(族ID,评分向量)
    val afterClusterRDD: RDD[(Int, (String, linalg.Vector))] = userVectorsRDD.map(r => {
      (model.predict(r._2), r)
    })



    //6.从这里开始新思路。
    /**
      * 思路：
      *  1.生成该族所有所有用户距离中心点距离的倒数系数，作为权重系数。
      *  2.把族中每个用户评分的Item和Rating，然后，同时对rating*权重系数，最后，累加获得族中用户推荐列表。
      *  3.存储该推荐列表，然后用于预测。
      * */

    val userWeight= afterClusterRDD.map(r=>{
      //r._1 //clusterIDX
      //r._2._1 //用户的ID
      //r._2._2 //用户的评分向量
      val centerVector=model.clusterCenters.apply(r._1)
      val distance=getDistance(centerVector,r._2._2)
      (r._2._1,1/distance)
    }).collectAsMap()

    //累加器
    val pureClusterAccumulator = new PureClusterAccumulator
    sc.register(pureClusterAccumulator, "pureClusterAccumulator")

    for(idx <- 0 until ap.k){
      val userSet=afterClusterRDD.filter(_._1==idx).map(_._2._1).collect().toSet
      //这里需要一个
      // 数据格式： (idx,itemId,score)
      val clustersItems: RDD[(Int, String, Double)] =pd.ratings.filter(r=>userSet.contains(r.user)).map(r=>{
        (r.item,r.rating * userWeight(r.user))
      }).reduceByKey(_+_).sortBy(_._2).map(r=>{
        (idx,r._1,r._2)
      })
      clustersItems.foreach(r=>{
        pureClusterAccumulator.add(r)
      })
      logger.info(s"累加器的记录条数:${pureClusterAccumulator.value.size}")

    }

    //生成用户所属族
    val userBelongClusterIndex: RDD[(String, Int)] = afterClusterRDD.map(r=>{
      (r._2._1,r._1)
    })

    new PureClusterModel(userBelongClusterIndex, sc.parallelize(pureClusterAccumulator.value.toSeq))
  }

  def getDistance(v1:linalg.Vector,v2:linalg.Vector):Double={
    var sum=0D
    for (idx <- 0 until v1.size) {
      sum += Math.pow(v1.apply(idx) - v2.apply(idx), 2)
    }
    if(sum==0)
      0
    else
      Math.sqrt(sum)
  }



  //实时从数据库获取用户的观看列表
  def getUserSaw(query: Query): Set[String] = {
    //TODO 比较合理的办法是，获取最后时间戳，然后传入。由训练时获取的已经收集的数据和新增数据两部分组成。
    //这里简化了逻辑，获取该用户的全部记录
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(Seq("rate")),
        targetEntityType = Some(Some("item")),
        limit = Some(-1),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(400, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"读取最近的事件记录时，时间过长！" +
          s" Empty list is used. $e ")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"读取最近的事件记录时发生异常: $e ")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Exception =>
          logger.error(s"Can't get targetEntityId of event $event.")
          throw e

      }
    }.toSet

    recentItems
  }

  override def predict(model: PureClusterModel, query: Query): PredictedResult = {


    //1.查看当前用户所属的族
    val userId2ClusterIndex= model.userBelongClusterIndex.collectAsMap()
    //1. 查看用户是否有相似度用户
    if(!userId2ClusterIndex.contains(query.user)){
      //该用户没有所属的族ID
      logger.warn(s"该用户:${query.user}没有该用户没有所属的簇，无法生成推荐！")
      return PredictedResult(Array.empty)
    }
    //2. 获取推荐列表
    val cIdx=userId2ClusterIndex(query.user)
    val clusterItems =model.clusterItems.filter(r=>{
      //r._1 //val key = s",c$clusterIdx,$itemID,"
      r._1.indexOf(s",c$cIdx,")> -1
    }).map(r=>{
      val itemid=r._1.replace(s",c$cIdx,","").replace(",","")
      (itemid,r._2)
    })
    logger.info(s"${query.user}所在簇的推荐列表的长度为：${clusterItems.count()}")

    //用户的已经观看列表
    val currentUserSawSet = getUserSaw(query)
    logger.info(s"已经观看的列表长度为:${currentUserSawSet.size}")

    val result = clusterItems.filter(r => !currentUserSawSet.contains(r._1))
    logger.info(s"生成的推荐列表的长度:${result.count()}")

    val sum: Double = result.map(r => r._2).sum
    if (sum == 0) return PredictedResult(Array.empty)

    val weight = 1.0
    val returnResult = result.map(r => {
      //val t: (String, Double) =r
      ItemScore(r._1, r._2 / sum * weight)
    }).sortBy(r => r.score, ascending = false).take(query.num)

    //排序，返回结果
    PredictedResult(returnResult)

  }

  override def batchPredict(m: PureClusterModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    val queryArray = qs.collect()

    val result = new ArrayBuffer[(Long, PredictedResult)]()

    for (r <- queryArray) {
      logger.info(s"Index:${r._1}," + r._2)
      val pred = predict(m, r._2)
      result.append((r._1, pred))
      logger.info(pred)
    }
    logger.info(s"result的大小:${result.length}")
    qs.sparkContext.parallelize(result)
  }
}


class PureClusterAccumulator
  extends AccumulatorV2[(Int, String, Double),mutable.Map[String, Double]]
    with Serializable {
  private val mapAccumulator = mutable.Map[String, Double]()
  override def isZero: Boolean = {
    mapAccumulator.isEmpty
  }

  override def copy(): AccumulatorV2[(Int, String, Double), mutable.Map[String, Double]] = {
    val newMapAccumulator = new PureClusterAccumulator()
    mapAccumulator.foreach(x => newMapAccumulator.add(x))
    newMapAccumulator
  }

  override def reset(): Unit ={
    mapAccumulator.clear()
  }


  override def add(v: (Int, String, Double)): Unit = {
    val clusterIdx = v._1
    val itemID = v._2
    val score = v._3
    val key = s",c$clusterIdx,$itemID,"
    if (!mapAccumulator.contains(key)) {
      mapAccumulator += key -> score
    } else {
      //选大的
      val oldScore= mapAccumulator(key)
      if(oldScore<score){
        mapAccumulator.put(key, score)
      }
    }
  }

  def add(v: (String, Double)): Unit = {
    val key = v._1
    val value = v._2
    if (!mapAccumulator.contains(key))
      mapAccumulator += key -> value
    else
      mapAccumulator.put(key, value)
  }

  override def merge(other: AccumulatorV2[(Int, String, Double), mutable.Map[String, Double]]): Unit = {
    other.value.foreach(r => {
      this.add(r)
    })
  }

  override def value: mutable.Map[String, Double] = {
    mapAccumulator
  }
}

