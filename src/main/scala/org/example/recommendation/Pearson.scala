package org.example.recommendation

import scala.collection.mutable

/**
  * @Author:Administrator
  * @Date:2019-07-19 18:44:04
  * @Description:
  * 主要用来处理生成Pearson系数和Pearson相关的对象。
  */
class Pearson(val pearsonThreasholds: Int, val topNLikes: Int) {


  def getPearsonNearstUsers(userRatings: Map[String, Iterable[Rating]]): (Map[String, List[Rating]],mutable.Map[String, List[(String, Double)]]) = {
    //1.获取用户的ID
    val users = userRatings.keySet.toList.sortBy(_.toInt)

    val userNearestPearson = new mutable.HashMap[String, List[(String, Double)]]()
    for {
      u1 <- users
    } {
      val maxPearson: mutable.Map[String, Double] = mutable.HashMap.empty
      for {u2 <- users
           if u1 < u2} {
        val ps = getPearson(u1, u2, userRatings)
        if (ps > 0) {
          //有用的相似度
          if (maxPearson.size < topNLikes) {
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

      userNearestPearson.put(u1, maxPearson.toList.sortBy(_._2).reverse)
    }

    //2.从用户的观看记录中选择用户喜欢的电影,用于后续的用户与用户之间的推荐
    val userLikesBeyondMean: Map[String, List[Rating]] = userRatings.map(r => {

      //当前用户的平均评分
      val sum = r._2.map(r => r.rating).sum
      val count = r._2.size

      //用户浏览的小于numNearst，全部返回
      val userLikes = if (count < topNLikes) {
        //排序后，直接返回
        r._2.toList.sortBy(_.rating).reverse
      } else {
        val mean = sum / count
        r._2.filter(t => t.rating > mean).toList.sortBy(_.rating).reverse.take(topNLikes)
      }

      (r._1, userLikes)
    })

    (userLikesBeyondMean,userNearestPearson)
  }

  /**
    * Pearson相似度计算公式:
    * r=sum((x-x_mean)*(y-y_mean))/(Math.pow(sum(x-x_mean),0.5)*Math.pow(sum(y-y_mean),0.5))
    * 要求，两者的共同因子必须达到阀值。默认为10
    **/
  private def getPearson(
                          userid1: String,
                          userid2: String,
                          userHashRatings: Map[String, Iterable[Rating]]): Double = {

    if (!userHashRatings.contains(userid1) || !userHashRatings.contains(userid2)) {
      //不相关
      return 0D
    }

    val user1Data = userHashRatings(userid1)
    val user2Data = userHashRatings(userid2)

    //1.求u1与u2共同的物品ID
    val comItemSet = user1Data.map(r => r.item).toSet.intersect(user2Data.map(r => r.item).toSet)
    if (comItemSet.size < pearsonThreasholds) {
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
    comItems.foreach(i => {

      val x_vt = i._2._1 - x_mean
      val y_vt = i._2._2 - y_mean
      xy += x_vt * y_vt

      x_var += x_vt * x_vt
      y_var += y_vt * y_vt
    })

    //Pearson系数
    xy / (Math.pow(x_var, 0.5) * Math.pow(y_var, 0.5))
  }
}
