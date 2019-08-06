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
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration


/**
  * Author:IceS
  * Date:2019-08-05 18:22:32
  * Description:
  * 基于聚类求相似度的算法
  */
case class ClusterAlgorithmParams(
                                   appName:String,
                                   k: Int = 10,
                                   maxIterations: Int = 20,
                                   numNearestUsers: Int = 60,
                                   numUserLikeMovies: Int = 100) extends Params

class ClusterAlgorithm(val ap: ClusterAlgorithmParams) extends PAlgorithm[PreparedData, ClusterModel, Query, PredictedResult] {
  @transient lazy val logger: Logger = Logger[this.type]

  override def train(sc: SparkContext, pd: PreparedData): ClusterModel = {

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
        |GROUP BY userId
        |ORDER BY userId ASC
      """.stripMargin)
    userVectorsDF.createOrReplaceTempView("uv")

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
    }).cache()
    val featuresRDD = userVectorsRDD.map(_._2)

    //3.准备聚类
    val bkm = new BisectingKMeans().setK(ap.k).setMaxIterations(ap.maxIterations)
    val model = bkm.run(featuresRDD)

    //4.聚类用户评分向量(族ID,评分向量)
    val afterClusterRDD: RDD[(Int, (String, linalg.Vector))] = userVectorsRDD.map(r => {
      (model.predict(r._2), r)
    })

    //5.生成用户喜欢的电影
    val userLikedRDD: RDD[(String, Seq[Rating])] = userLikedItems(ap.numUserLikeMovies, pd.ratings)
    //6.根据用户评分向量生成用户最邻近用户的列表
    val nearestUser: mutable.Map[String, List[(String, Double)]] = userNearestTopN(ap.numNearestUsers, afterClusterRDD)

    new ClusterModel(userLikedRDD,sc.parallelize(nearestUser.toSeq))
  }

  def userNearestTopN(numNearestUsers: Int, clustedRDD: RDD[(Int, (String, linalg.Vector))]): mutable.Map[String, List[(String, Double)]] = {
    //clustedRDD: RDD[(Int, (Int, linalg.Vector))]
    //                簇Index  Uid    评分向量
    val users = clustedRDD.map(r => {
      (r._1, r._2._1)
    }).collect()

    val userNearestPearson: mutable.Map[String, List[(String, Double)]] = new mutable.HashMap[String, List[(String, Double)]]()
    for {
      (cIdx, uid) <- users
    } {

      val maxPearson: mutable.Map[String, Double] = mutable.HashMap.empty
      val cUsers: RDD[(String, linalg.Vector)] = clustedRDD.filter(_._1 == cIdx).map(_._2).cache()

      //当前用户的评分向量
      val v1: linalg.Vector = cUsers.filter(_._1 == uid).map(_._2).first()

      for ((u2, v2) <- cUsers) {

        val ps = getCosineSimilarity(v1, v2)
        if (ps > 0) {
          //有用的相似度
          if (maxPearson.size < numNearestUsers) {
            maxPearson.put(u2, ps)
          } else {
            val min_p = maxPearson.map(r => (r._1, r._2)).minBy(r => r._2)
            if (ps > min_p._2) {
              maxPearson.remove(min_p._1)
              maxPearson.put(u2, ps)
            }

          }
        }
      }

      logger.info(s"user:$uid nearest pearson users count:${maxPearson.count(_ => true)}")
      userNearestPearson.put(uid, maxPearson.toList.sortBy(_._2).reverse)
    }
    userNearestPearson
  }

  //尝试cos相似度
  def getCosineSimilarity(v1: linalg.Vector, v2: linalg.Vector): Double = {
    var sum = 0D
    var v1Len = 0D
    var v2Len = 0D
    for (idx <- 0 until v1.size) {
      sum += v1.apply(idx) * v2.apply(idx)
      v1Len += Math.pow(v1.apply(idx), 2)
      v2Len += Math.pow(v2.apply(idx), 2)
    }
    if (v1Len == 0 || v2Len == 0)
      0D
    else
      sum / (Math.sqrt(v1Len) * Math.sqrt(v2Len))
  }

  def userLikedItems(numUserLikeMovies: Int, data: RDD[Rating]): RDD[(String, Seq[Rating])] = {

    val groupRDD: RDD[(String, Iterable[Rating])] = data.groupBy(_.user)
    //1.计算用户的平均分
    val userMean = groupRDD.map(r => {
      val sum = r._2.map(r2 => r2.rating).sum
      val count = r._2.size
      //用户浏览的小于numNearst，全部返回
      val userLikes: Seq[Rating] = if (count < numUserLikeMovies) {
        //排序后，直接返回
        r._2.toList.sortBy(_.rating).reverse
      } else {
        val mean = sum / count
        r._2.filter(t => t.rating > mean).toList.sortBy(_.rating).reverse.take(numUserLikeMovies)
      }

      (r._1, userLikes)
    })

    userMean.persist()
  }

  //实时从数据库获取用户的观看列表
  def getUserSaw(query: Query):Set[String]={
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
        case e: Exception => {
          logger.error(s"Can't get targetEntityId of event $event.")
          throw e
        }
      }
    }.toSet

    recentItems
  }
  override def predict(model: ClusterModel, query: Query): PredictedResult = {

    //1. 查看用户是否有相似度用户
    val userNearestRDD: RDD[(String, List[(String, Double)])] = model.nearestUserRDD.filter(_._1==query.user)
    if(userNearestRDD.count()==0){
      //该用户没有最相似的用户列表
      logger.warn(s"该用户:${query.user}没有相似用户列表，无法生成推荐！")
      return PredictedResult(Array.empty)
    }

    //2. 获取推荐列表
    //用户相似度的Map
    val userNearestMap=userNearestRDD.flatMap(_._2).collectAsMap()
    //用户的已经观看列表
    val currentUserSawSet=getUserSaw(query)
    val result=model.userLikedRDD.filter(r=> userNearestMap.contains(r._1)).
      flatMap(_._2).
      filter(r=> !currentUserSawSet.contains(r.item)).
      map(r=>{
      //r.rating
        //r.item
        //userNearestMap(r.user)
        (r.item,r.rating * userNearestMap(r.user))
    }).reduceByKey(_+_)

    val sum:Double = result.map(r => r._2).sum
    if (sum == 0) return PredictedResult(Array.empty)

    val weight = 1.0
    val returnResult = result.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    }).sortBy(r => r.score, false).take(query.num)

    //排序，返回结果
    PredictedResult(returnResult)

  }

  override def batchPredict(m: ClusterModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
    val queryArray= qs.collect()

    val result = new ArrayBuffer[(Long, PredictedResult)]()

    for(r <-queryArray){
      logger.info(s"Index:${r._1},"+r._2)
      val pred=predict(m, r._2)
      result.append((r._1, pred))
      logger.info(pred)
    }
    logger.info(s"result的大小:${result.length}")
    qs.sparkContext.parallelize(result)
  }
}
