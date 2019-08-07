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
import org.apache.spark.util.AccumulatorV2

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
                                   appName: String,
                                   k: Int = 10,
                                   method:String="cosine",
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
    val bkm = new BisectingKMeans().setK(ap.k).setMaxIterations(ap.maxIterations)
    val model = bkm.run(featuresRDD)

    //调试信息
    model.clusterCenters.foreach(println)

    //4.聚类用户评分向量(族ID,评分向量)
    val afterClusterRDD: RDD[(Int, (String, linalg.Vector))] = userVectorsRDD.map(r => {
      (model.predict(r._2), r)
    })

    //5.从这里开始新思路。
    /**
      * 思路：
      *  1.生成该族所有所有用户距离中心点距离的倒数系数，作为权重系数。
      *  2.把族中每个用户评分的Item和Rating，然后，同时对rating*权重系数，最后，累加获得族中用户推荐列表。
      *  3.存储该推荐列表，然后用于预测。
      * */

    //5.生成用户喜欢的电影
    val userLikedRDD: RDD[(String, Seq[Rating])] = userLikedItems(ap.numUserLikeMovies, pd.ratings)
    //调试信息
    logger.info("userLikedRDD.count(): " + userLikedRDD.count())

    //6.根据用户评分向量生成用户最邻近用户的列表
    val nearestUser: mutable.Map[String, Double] = userNearestTopN(ap.method,ap.k,ap.numNearestUsers, afterClusterRDD, sc)
    //调试信息
    logger.info("nearestUser.count():" + nearestUser.size)
    nearestUser.take(10).foreach(println)

    new ClusterModel(userLikedRDD, sc.parallelize(nearestUser.toSeq))
  }

  def userNearestTopN(method:String,k: Int, numNearestUsers: Int, clustedRDD: RDD[(Int, (String, linalg.Vector))], sc: SparkContext): mutable.Map[String, Double] = {
    //clustedRDD: RDD[(Int, (Int, linalg.Vector))]
    //                簇Index  Uid    评分向量


    val userNearestAccumulator = new NearestUserAccumulator
    sc.register(userNearestAccumulator, "userNearestAccumulator")

    //1.考虑从族中进行计算相似度
    for(idx <- 0 until k){

      val curUsersVectors = clustedRDD.filter(_._1 == idx).map(_._2).map(r =>(r._1.toInt, r._2)).cache()
      logger.info(s"族$idx 中用户数量为：${curUsersVectors.count()}")

      val uids: Array[(Int, linalg.Vector)] =curUsersVectors.sortBy(_._1).collect()

      for {
        (u1,v1) <- uids
        (u2,v2) <- uids
        if u1 < u2
      } {

        val score =if(method=="cosine"){
          getCosineSimilarity(v1,v2)
        } else {
          getImprovePearson(v1, v2)
        }
        //logger.info(s"score:$score")
        if (score > 0) {

          //限制u1相似度列表的大小
          val u1SCount = userNearestAccumulator.value.count(r => r._1.indexOf(s",$u1,") > -1)
          //限制u2相似度列表的大小
          val u2SCount = userNearestAccumulator.value.count(r => r._1.indexOf(s",$u2,") > -1)
          //logger.info(s"u1SCount:$u1SCount,u2SCount:$u2SCount")


          if (u1SCount < numNearestUsers && u2SCount < numNearestUsers) {
            userNearestAccumulator.add(u1, u2, score)
          } else {

            if (u1SCount >= numNearestUsers) {
              //选择小的替换
              val min_p: (String, Double) = userNearestAccumulator.value.filter(r => r._1.indexOf("," + u1 + ",") > -1).minBy(_._2)
              if (score > min_p._2) {
                userNearestAccumulator.value.remove(min_p._1)
                userNearestAccumulator.add(u1, u2, score)
              }
            }

            if (u2SCount >= numNearestUsers) {
              //选择小的替换
              val min_p: (String, Double) = userNearestAccumulator.value.filter(r => r._1.indexOf("," + u2 + ",") > -1).minBy(_._2)
              if (score > min_p._2) {
                userNearestAccumulator.value.remove(min_p._1)
                userNearestAccumulator.add(u1, u2, score)
              }
            }
          }
        }//end  if (score > 0) {
      }
      logger.info(s"累加器数据条数：${userNearestAccumulator.value.size}条记录.")
    }

    userNearestAccumulator.value
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

  /** 改进Pearson算法
    * r=sum((x-x_mean)*(y-y_mean))/(Math.pow(sum(x-x_mean),0.5)*Math.pow(sum(y-y_mean),0.5))
    * */
  def getImprovePearson(v1: linalg.Vector, v2: linalg.Vector): Double = {
    //偏差因子
    var w=0.0


    var sum1 = 0D
    var sum2 = 0D
    for (idx <- 0 until v1.size) {
      sum1 += v1.apply(idx)
      sum2 += v2.apply(idx)

      w+=Math.pow(v1.apply(idx)-v2.apply(idx),2)
    }
    val mean1 = sum1 / v1.size
    val mean2 = sum2 / v2.size
    var sum = 0D
    sum1 = 0
    sum2 = 0
    for (idx <- 0 until v1.size) {
      sum += (v1.apply(idx) - mean1) * (v2.apply(idx) - mean2)
      sum1 += Math.pow(v1.apply(idx) - mean1,2)
      sum2 += Math.pow(v2.apply(idx) - mean2,2)
    }
    val sum1sum2 = Math.sqrt(sum1) * Math.sqrt(sum2)

    //计算偏差指数
    w = Math.pow(Math.E,Math.sqrt(w)*(-1)/v1.size)

    if (sum1sum2 == 0)
      0
    else
      sum / sum1sum2 *w

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

  override def predict(model: ClusterModel, query: Query): PredictedResult = {

    //1. 查看用户是否有相似度用户
    val userNearestRDD = model.nearestUserRDD.filter(r => {
      r._1.indexOf(s",${query.user},") > -1
    })
    if (userNearestRDD.count() == 0) {
      //该用户没有最相似的用户列表
      logger.warn(s"该用户:${query.user}没有相似用户列表，无法生成推荐！")
      return PredictedResult(Array.empty)
    }

    //2. 获取推荐列表
    //用户相似度的Map
    val userNearestMap = userNearestRDD.map(r => {
      val uid = r._1.replace(s",${query.user},", "").replace(",", "")
      (uid, r._2)
    }).sortBy(_._2, ascending = false).collectAsMap()
    logger.info(s"${query.user}的相似用户列表的长度为：${userNearestMap.size}")

    //用户的已经观看列表
    val currentUserSawSet = getUserSaw(query)
    logger.info(s"已经观看的列表长度为:${currentUserSawSet.size}")
    val result = model.userLikedRDD.filter(r => userNearestMap.contains(r._1)).
      flatMap(_._2).
      filter(r => !currentUserSawSet.contains(r.item)).
      map(r => {
        //r.rating
        //r.item
        //userNearestMap(r.user)
        (r.item, r.rating * userNearestMap(r.user))
      }).reduceByKey(_ + _)
    logger.info(s"生成的推荐列表的长度:${result.count()}")
    val sum: Double = result.map(r => r._2).sum
    if (sum == 0) return PredictedResult(Array.empty)

    val weight = 1.0
    val returnResult = result.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    }).sortBy(r => r.score, ascending = false).take(query.num)

    //排序，返回结果
    PredictedResult(returnResult)

  }

  override def batchPredict(m: ClusterModel, qs: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {
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

class NearestUserAccumulator extends AccumulatorV2[(Int, Int, Double), mutable.Map[String, Double]] with Serializable {
  private val mapAccumulator = mutable.Map[String, Double]()

  def containsKey(k1: Int, k2: Int): Boolean = {
    val key1 = s",$k1,$k2,"
    val key2 = s",$k2,$k1,"
    mapAccumulator.contains(key1) || mapAccumulator.contains(key2)
  }

  override def isZero: Boolean = {
    mapAccumulator.isEmpty
  }

  override def copy(): AccumulatorV2[(Int, Int, Double), mutable.Map[String, Double]] = {
    val newMapAccumulator = new NearestUserAccumulator()
    mapAccumulator.foreach(x => newMapAccumulator.add(x))
    newMapAccumulator
  }

  override def reset(): Unit = {
    mapAccumulator.clear()
  }

  override def add(v: (Int, Int, Double)): Unit = {
    val u1 = v._1
    val u2 = v._2
    val score = v._3
    if (!this.containsKey(u1, u2)) {
      val key = s",$u1,$u2,"
      mapAccumulator += key -> score
    } else {
      val key1 = s",$u1,$u2,"
      val key2 = s",$u2,$u1,"
      if (mapAccumulator.contains(key1)) {
        mapAccumulator.put(key1, score)
      } else if (mapAccumulator.contains(key2)) {
        mapAccumulator.put(key2, score)
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

  override def merge(other: AccumulatorV2[(Int, Int, Double), mutable.Map[String, Double]]): Unit = {
    other.value.foreach(r => {
      this.add(r)
    })
  }

  override def value: mutable.Map[String, Double] = {
    mapAccumulator
  }
}