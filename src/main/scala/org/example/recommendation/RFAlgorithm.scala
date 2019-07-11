package org.example.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest



case class RFAlgorithmParams(
                              numTrees: Int,
                              featureSubsetStrategy: String ="auto",
                              impurity: String ="gini",
                              maxDepth: Int =5,
                              maxBins: Int =100) extends Params

class RFAlgorithm(val rdp: RFAlgorithmParams) extends PAlgorithm[PreparedData, RFModel, Query, PredictedResult] {
  @transient lazy val logger = Logger[this.type]

  override def train(sc: SparkContext, pd: PreparedData): RFModel = {
    //1.计算用户的平均分
    val userMean =pd.ratings.map(r=>(r.user,(r.rating,1))).reduceByKey((l,r)=>{
      (l._1+r._1,l._2+r._2)
    }).map(r=>{
      (r._1,r._2._1/r._2._2)
    }).collectAsMap()

    //2.处理处理数据格式
    val trainingData=pd.ratings.map(r=>{
      val like=if(r.rating>userMean(r.user)) 1.0 else 0D
      LabeledPoint(like,Vectors.dense(r.user.toInt,r.item.toInt))
    })

    //3.准备模型参数
    //分类数目
    val numClass=2
    //设定输入数据格式
    val categoricalFeaturesInfo=Map[Int,Int]()


    val model=RandomForest.trainClassifier(trainingData,numClass,categoricalFeaturesInfo,rdp.numTrees,rdp.featureSubsetStrategy,rdp.impurity,rdp.maxDepth,rdp.maxBins)




  }

  override def predict(model: RFModel, query: Query): PredictedResult = ???
}
