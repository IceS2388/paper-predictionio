package org.example.recommendation

import grizzled.slf4j.Logger

import scala.collection.mutable

/**
  * Author:Administrator
  * Date:2019-07-19 18:44:04
  * Description:
  * 主要用来处理生成Pearson系数和Pearson相关的对象。
  */
class SimilarityFactor(val pearsonThreashold: Int, val numNearestUsers: Int, val numUserLikeMovies:Int) {
  @transient lazy val logger: Logger = Logger[this.type]

  def getNearstUsers(userRatings: Map[String, Iterable[Rating]]): (Map[String, List[Rating]],mutable.Map[String, List[(String, Double)]]) = {
    //1.获取用户的ID
    val users = userRatings.keySet.toList.sortBy(_.toInt)

    val userNearestPearson = new mutable.HashMap[String, List[(String, Double)]]()
    for {
      u1 <- users
    } {
      val maxPearson: mutable.Map[String, Double] = mutable.HashMap.empty
      for {u2 <- users
           if u1 < u2} {
        val ps = getSimilarity(u1, u2, userRatings)
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

      //logger.info(s"user:$u1 nearest pearson users count:${maxPearson.count(_=>true)}")
      userNearestPearson.put(u1, maxPearson.toList.sortBy(_._2).reverse)
    }

    //2.从用户的观看记录中选择用户喜欢的电影,用于后续的用户与用户之间的推荐
    val userLikesBeyondMean: Map[String, List[Rating]] = userRatings.map(r => {

      //当前用户的平均评分
      val sum = r._2.map(r => r.rating).sum
      val count = r._2.size

      //用户浏览的小于numNearst，全部返回
      val userLikes = if (count < numUserLikeMovies) {
        //排序后，直接返回
        r._2.toList.sortBy(_.rating).reverse
      } else {
        val mean = sum / count
        r._2.filter(t => t.rating > mean).toList.sortBy(_.rating).reverse.take(numUserLikeMovies)
      }

      //logger.info(s"user:${r._1} likes Movies Count ${userLikes.count(_=>true)}")

      (r._1, userLikes)
    })



    (userLikesBeyondMean,userNearestPearson)
  }

  /**
    * Pearson相似度计算公式:
    * r=sum((x-x_mean)*(y-y_mean))/(Math.sqrt(sum(Math.pow(x-x_mean,2)))*Math.sqrt(sum(Math.pow(y-y_mean,2))))
    * 要求，两者的共同因子必须达到阀值。默认为10
    *
    * 增加评分差距因子。2019年7月26日
    **/
  private def getSimilarity(
                          userid1: String,
                          userid2: String,
                          userHashRatings: Map[String, Iterable[Rating]]): Double = {

    if (!userHashRatings.contains(userid1) || !userHashRatings.contains(userid2)) {
      //不相关
      return 0D
    }

    val user1Data: Iterable[Rating] = userHashRatings(userid1)
    val user2Data: Iterable[Rating] = userHashRatings(userid2)

    //1.求u1与u2共同的物品ID
    val comItemSet = user1Data.map(r => r.item).toSet.intersect(user2Data.map(r => r.item).toSet)
    if (comItemSet.size < pearsonThreashold) {
      //小于共同物品的阀值，直接退出
      return 0D
    }

    val user1ComData = user1Data.filter(r => comItemSet.contains(r.item)).map(r => (r.item, r.rating)).toMap
    val user2ComData = user2Data.filter(r => comItemSet.contains(r.item)).map(r => (r.item, r.rating)).toMap

    //2.把共同物品转变为对应的评分
    val comItems = comItemSet.map(r => (r, (user1ComData(r), user2ComData(r))))

    //计算平均值和标准差
    val count = comItems.size
    val sum1 = comItems.map(item => item._2._1).sum
    val sum2 = comItems.map(item => item._2._2).sum

    //平均值
    val x_mean = sum1 / count
    val y_mean = sum2 / count

    //标准差
    var xy = 0D
    var x_var = 0D
    var y_var = 0D

    //偏差因素
    var w=0.0
    comItems.foreach(i => {
      //         item  u1_rating u2_rating
      //val t: (String, (Double, Double))
      w+=Math.pow(i._2._1-i._2._2,2)

      //计算Pearson系数
      val x_vt = i._2._1 - x_mean
      val y_vt = i._2._2 - y_mean
      xy += x_vt * y_vt

      x_var += Math.pow(x_vt -x_mean,2)
      y_var += Math.pow(y_vt -y_mean,2)
    })

    //计算偏差指数
    w = Math.pow(Math.E,Math.sqrt(w)*(-1)/count)

    //Pearson系数
    val pearson=xy / (Math.sqrt(x_var) * Math.sqrt(y_var))

    //改良过后的相似度计算方法
    pearson*w
  }
}
