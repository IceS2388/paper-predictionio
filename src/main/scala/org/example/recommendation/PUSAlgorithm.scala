package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.example.recommendation._

import scala.collection.mutable


/**
  * 全称：PearsonUserCorrelationSimilarity
  *
  * @param pearsonThreasholds 计算Pearson系数时，最低用户之间共同拥有的元素个数。若相同元素个数的阀值，低于该阀值，相似度为0.
  * @param topNLikes          Pearson相似度最大的前N个用户
  **/
case class PUSAlgorithmParams(pearsonThreasholds: Int, topNLikes: Int) extends Params


/** 功能：
  * 实现用户Pearson相似度算法。
  *
  * @param rank 取相似度最大的前N个用户
  *             思路：
  *             训练阶段:
  *   1.根据输入的数据，获取用户与用户之间的Pearson相似度。
  *   2.模型中存储前N个最相似的用户和Pearson相似度
  *   3.根据模型选择前N个用户，筛选其超过平均值的电影评分。
  *             预测阶段：
  *   1.根据模型中存储的电影来进行推荐。相似度=pearson系数*相似用户对其的评分
  * */
class PUSAlgorithm(val ap: PUSAlgorithmParams) extends PAlgorithm[PreparedData, PUSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]


  /** *
    * 训练阶段:
    *  1.根据输入的数据，获取用户与用户之间的Pearson相似度。
    *  2.模型中存储前N个最相似的用户和Pearson相似度
    *  3.根据模型选择前N个用户，筛选其超过平均值的电影评分。
    */
  override def train(sc: SparkContext, data: PreparedData): PUSModel = {

    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    logger.info(s"1.进入train方法。原始数据条数：${data.ratings.count()}")

    //1.转换为HashMap,方便计算Pearson相似度
    val userRatings = data.ratings.groupBy(r => r.user).collectAsMap().toMap


    //2.获取用户ID的向量
    val users = userRatings.keySet.toList.sortBy(_.toDouble)
    logger.info(s"2.userMap被初始化后元素的个数：${users.size}")

    val userNearestPearson = new mutable.HashMap[String, List[(String, Double)]]()

    for {
      u1 <- users
    } {
      val maxPearson: mutable.Map[String, Double] = mutable.HashMap.empty
      for {u2 <- users
           if (u1 < u2)} {
        val ps = getPearson(u1, u2, userRatings)
        if (ps > 0) {
          //有用的相似度
          if (maxPearson.size < ap.topNLikes) {
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
      logger.info(s"2.1maxPearson.size:${maxPearson.size}")
      userNearestPearson.put(u1, maxPearson.toList.sortBy(_._2).reverse)
    }
    logger.info(s"3.userNearestPearson用户Pearson相似度最大的个数：${userNearestPearson.size}")


    //3.生成指定用户喜欢的电影
    val userLikesBeyondMean = userRatings.map(r => {

      var sum = 0D
      var count = 0
      r._2.map(rt => {
        sum += rt.rating
        count += 1
      })

      //用户浏览的小于numNearst，全部返回
      val userLikes = if (count < ap.topNLikes) {
        r._2.toList.sortBy(_.rating).reverse
      } else {
        val mean = sum / count
        r._2.filter(t => t.rating > mean).toList.sortBy(_.rating).reverse.take(ap.topNLikes)
      }

      (r._1, userLikes)
    })
    logger.info(s"4.userLikes用户最喜欢的电影个数：${userLikesBeyondMean.size}")
    new PUSModel(sc.parallelize(userRatings.toSeq), sc.parallelize(userNearestPearson.toSeq), sc.parallelize( userLikesBeyondMean.toSeq))

  }

  /**
    * Pearson相似度计算公式:
    * r=sum((x-x_mean)*(y-y_mean))/(Math.pow(sum(x-x_mean),0.5)*Math.pow(sum(y-y_mean),0.5))
    * 要求，两者的共同因子必须达到阀值。默认为10
    **/
  private def getPearson(userid1: String,
                         userid2: String,
                         userHashRatings: Map[String, Iterable[Rating]]): Double = {
    if (!userHashRatings.contains(userid1) || !userHashRatings.contains(userid2)) {
      //不相关
      return 0D
    }

    //u1与u2共同的物品ID
    var comMap = new mutable.HashMap[String, (Double, Double)]

    userHashRatings.get(userid1).get.map(u1 => {
      //添加u1拥有的物品
      comMap.put(u1.item, (u1.rating, -1D))
    })

    userHashRatings.get(userid2).get.map(u2 => {
      //添加u2拥有的物品
      if (comMap.contains(u2.item)) {
        val u1_rating = comMap.get(u2.item).get._1
        comMap.update(u2.item, (u1_rating, u2.rating))
      }
    })

    //两者共同的拥有的物品及其评分
    val comItems = comMap.filter(r => r._2._1 >= 0 && r._2._2 >= 0)

    //小于阀值，直接返回
    if (comItems.size < ap.pearsonThreasholds) {
      return 0D
    }


    //计算平均值和标准差
    var count = 0
    var sum1 = 0D
    var sum2 = 0D

    comItems.map(i => {
      sum1 += i._2._1
      sum2 += i._2._2
      count += 1
    })

    //平均值
    val x_mean = sum1 / count
    val y_mean = sum2 / count

    //标准差
    var xy = 0D
    var x_var = 0D
    var y_var = 0D
    comItems.map(i => {
      val x_vt = i._2._1 - x_mean
      val y_vt = i._2._2 - y_mean
      xy += x_vt * y_vt

      x_var += x_vt * x_vt
      y_var += y_vt * y_vt
    })

    //Pearson系数
    xy / (Math.pow(x_var, 0.5) * Math.pow(y_var, 0.5))
  }


  override def predict(model: PUSModel, query: Query): PredictedResult = {
    val uMap=model.userMap.collectAsMap()
    if (!uMap.contains(query.user)) {
      //该用户没有过评分记录，返回空值
      logger.info(s"该用户没有过评分记录，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //1.获取Pearson最大的n个用户
    val userPearson = model.userNearestPearson.collectAsMap()
    if (!userPearson.contains(query.user)) {
      //该用户没有对应的Pearson相似用户
      logger.info(s"该用户没有相似的用户，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //2.用户最喜欢的前N部电影
    val userLikes = model.userLikesBeyondMean.collectAsMap()

    val sawItem = uMap.get(query.user).get.map(r => (r.item, r.rating)).toMap

    val result = new mutable.HashMap[String, Double]()
    //这是与用户最相近的前N个用户
    userPearson.get(query.user).get.map(r => {
      //r._1 //相似的userID
      // r._2 //相似度
      if (userLikes.contains(r._1)) {
        //该用户有最喜欢的电影记录
        userLikes.get(r._1).get.map(r1 => {
          //r1.item
          //r1.rating
          if (!sawItem.contains(r1.item)) {
            //没看过的
            if (result.contains(r1.item)) {
              logger.info(s"item:${r1.item},old scores:${result.get(r1.item).get},new scores:${r1.rating * r._2}")
              //这是已经在推荐列表中
              result.update(r1.item, result.get(r1.item).get + r1.rating * r._2)
            } else {
              result.put(r1.item, r1.rating * r._2)
            }
          }
        })
      }
    })
    val preResult=result.map(r => (r._1, r._2)).toList.sortBy(_._2).reverse.take(query.num).map(r => new ItemScore(r._1, r._2))
    preResult.foreach(r=>{
      logger.info(s"item:${r.item},scores:${r.score}")
    })
    //排序，返回结果
    PredictedResult(preResult.toArray)
  }
}
