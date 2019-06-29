package paper.algorithm

import grizzled.slf4j.Logger
import org.apache.mahout.cf.taste.impl.common.FastByIDMap
import org.apache.mahout.cf.taste.impl.model.{GenericDataModel, GenericUserPreferenceArray}
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity
import org.apache.mahout.cf.taste.model.PreferenceArray
import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.example.recommendation.{PredictedResult, PreparedData, Query}


/**
  * @param rank         取相似度最大的前N个用户
  * @param userSimilary `true`表示用户之间的相似度；`false`表示物品之间的相似度
  * 思路：
  *   训练阶段:
  *   1.根据输入的数据，获取用户与用户之间的Pearson相似度。
  *   2.模型中存储前N个最相似的用户和Pearson相似度
  *   3.根据模型选择前N个用户，筛选其超过平均值的电影评分。
  *   预测阶段：
  *   1.根据模型中存储的电影来进行推荐。相似度=pearson系数*相似用户对其的评分
  **/
case class PearsonUserSimilarityAlgorithmParams(rank: Int, userSimilary: Boolean) extends Params

/**
  * 实现物品相似度算法
  *
  **/
class PearsonUserSimilarityAlgorithm(val ap: PearsonUserSimilarityAlgorithmParams) extends P2LAlgorithm[PreparedData, PearsonUserCorrelationSimilarityModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  if (ap.rank > 30) {
    //取最相似的前30位
    logger.warn(
      s"超过前30位用户之间的相似性有浪费空间的可能性。${ap.rank}"
    )


  }

  /***
    * 训练阶段:
    *  1.根据输入的数据，获取用户与用户之间的Pearson相似度。
    *  2.模型中存储前N个最相似的用户和Pearson相似度
    *  3.根据模型选择前N个用户，筛选其超过平均值的电影评分。
    */
  /
  override def train(sc: SparkContext, data: PreparedData): PearsonUserCorrelationSimilarityModel = {

    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")



    if (ap.userSimilary) {
      //用户相似度
      //该用户拥有物品的集合
      val userItemSet = data.ratings.groupBy(r => r.user)

      val preRDD = userItemSet.map(r => {
        val userid = r._1
        val ratings = r._2
        //计算总物品数
        var rcount = 0
        ratings.map(t => rcount += 1)

        var pre: PreferenceArray = new GenericUserPreferenceArray(rcount)
        var index = 0
        ratings.map(t => {
          pre.setUserID(index, t.user.toLong)
          pre.setItemID(index, t.item.toLong)
          pre.setValue(index, t.rating.toFloat)
          index += 1
        })
        pre
      })

      var preferences = new FastByIDMap[PreferenceArray]()
      var i = 0
      for (pre <- preRDD) {
        preferences.put(i, pre)
        i += 1
      }

      new PearsonUserCorrelationSimilarityModel(ap.rank,new PearsonCorrelationSimilarity(new GenericDataModel(preferences)))

    } else {
      //物品相似度
      //该物品
      val itemsUserSet = data.ratings.groupBy(r => r.item)

      val preRDD = itemsUserSet.map(r => {
        val itemid = r._1
        val ratings = r._2
        //计算总用户数
        var rcount = 0
        ratings.map(t => rcount += 1)

        var pre: PreferenceArray = new GenericUserPreferenceArray(rcount)
        var index = 0
        ratings.map(t => {
          //默认是以用户为基准
          pre.setUserID(index, t.item.toLong)
          pre.setItemID(index, t.user.toLong)
          pre.setValue(index, t.rating.toFloat)
          index += 1
        })
        pre
      })

      var preferences = new FastByIDMap[PreferenceArray]()
      var i = 0
      for (pre <- preRDD) {
        preferences.put(i, pre)
        i += 1
      }


      new PearsonUserCorrelationSimilarityModel(ap.rank,new PearsonCorrelationSimilarity(new GenericDataModel(preferences)))
    }

  }


  override def predict(model: PearsonUserCorrelationSimilarityModel, query: Query): PredictedResult = {
    PredictedResult
  }
}
