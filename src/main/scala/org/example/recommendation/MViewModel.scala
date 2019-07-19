package org.example.recommendation

import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 存储访问指定数量的最多电影
  * @param userMap 用户的观影记录
  * @param mostView 用户观看最多的电影
  * */
class MViewModel(
  val userMap: RDD[(String, Iterable[String])],
  val mostView:RDD[(String,Int)]) extends PersistentModel[MViewAlgorithmParams] {
  override def save(id: String, params: MViewAlgorithmParams, sc: SparkContext): Boolean = {
    userMap.saveAsObjectFile(s"/tmp/mv/${id}/userMap")
    mostView.saveAsObjectFile(s"/tmp/mv/${id}/mostView")
    true
  }
  override def toString: String = {
    s"userMap: RDD[(String, Iterable[Rating])]:${userMap.count()}"+
    s"mostView:RDD[(String,Int)]:${mostView.count()}"
  }
}
object MViewModel extends PersistentModelLoader[MViewAlgorithmParams, MViewModel]{

  override def apply(id: String, params: MViewAlgorithmParams, sc: Option[SparkContext]): MViewModel = {

    new MViewModel(
      userMap = sc.get.objectFile[(String, Iterable[String])](s"/tmp/mv/${id}/userMap"),
      mostView = sc.get.objectFile[(String,Int)](s"/tmp/mv/${id}/mostView")
    )
  }
}
