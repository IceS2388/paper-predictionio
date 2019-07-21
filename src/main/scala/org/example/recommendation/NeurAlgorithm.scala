package org.example.recommendation

import java.nio.file.Paths

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.datavec.api.records.reader.impl.csv.CSVRecordReader
import org.datavec.api.split.FileSplit
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.learning.config.Nesterovs
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction

import scala.collection.mutable


/**
  * Author:IceS
  * Date:2019-07-19 21:11:37
  * Description:
  * NONE
  */

case class NeurAlgorithmParams(pearsonThreasholds: Int, topNLikes: Int) extends Params

class NeurAlgorithm(val ap: NeurAlgorithmParams) extends PAlgorithm[PreparedData, NeurModel, Query, PredictedResult] {

  @transient lazy val logger: Logger = Logger[this.type]

  override def train(sc: SparkContext, data: PreparedData): NeurModel = {
    require(!data.ratings.take(1).isEmpty, "评论数据不能为空！")

    //1.转换为HashMap,方便计算Pearson相似度,这是个昂贵的操作
    val userRatings: Map[String, Iterable[Rating]] = data.ratings.groupBy(r => r.user).collectAsMap().toMap

    //2.计算用户与用户之间Pearson系数，并返回用户观看过后喜欢的列表和pearson系数最大的前TopN个用户的列表
    val userLikesAndNearstPearson = new Pearson(ap.pearsonThreasholds, ap.topNLikes).getPearsonNearstUsers(userRatings)

    //3.开始用单层神经网络
    //3.1 计算用户的平均分
    val userMean: Map[String, Double] = userRatings.map(r => {
      val sum = r._2.toSeq.map(r2 => r2.rating).sum
      val size = r._2.size
      (r._1, sum / size)
    })

    val rddWritables: RDD[String] = data.ratings.map(r => {
      val like = if (r.rating > userMean(r.user)) 1.0 else 0D
      s"$like,${r.user},${r.item}"
    })

    //保存到Hadoop或者本地文件，防止OOM
    val dataCSVPath = "/tmp/spark-warehouse/prediction.csv"

    val pcsdFile = Paths.get(dataCSVPath).toFile
    if (pcsdFile.exists()) {
      pcsdFile.delete()
    }
    //保存临时文件
    rddWritables.saveAsTextFile(dataCSVPath)

    //测试打印
    rddWritables.take(20) foreach println


    val labelIndex = 0
    val numLabelClasses = 2
    // 随机数种子
    val seed: Int = 123
    //学习速率
    val learningRate: Double = 0.006
    //批次大小
    val batchSize: Int = 50
    //周期
    val nEpochs: Int = 30
    //输入数据大小
    val numInputs: Int = 2
    //隐藏层的节点数量
    val numHiddenNodes = 20
    //输出数据
    val numOutputs: Int = 1

    val rr = new CSVRecordReader(',')
    rr.initialize(new FileSplit(pcsdFile))
    val trainIter = new RecordReaderDataSetIterator(rr, batchSize, labelIndex, numLabelClasses)


    logger.info("Build model....")
    val conf = new NeuralNetConfiguration.Builder()
      .seed(seed)
      .weightInit(WeightInit.XAVIER)
      // use stochastic gradient descent as an optimization algorithm
      .updater(new Nesterovs(learningRate, 0.9))
      .list()
      .layer(new DenseLayer.Builder() //create the first, input layer with xavier initialization
        .nIn(numInputs)
        .nOut(numHiddenNodes)
        .activation(Activation.RELU)
        .build())
      .layer(new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD) //create hidden layer
        .nIn(numHiddenNodes)
        .nOut(numOutputs)
        .activation(Activation.SOFTMAX)
        .build())
      .build()

    val model: MultiLayerNetwork = new MultiLayerNetwork(conf)
    model.init()
    //只是为了打印日志
    model.setListeners(new ScoreIterationListener(10))

    logger.info("Train model....")
    model.fit(trainIter, nEpochs)

    new NeurModel(sc.parallelize(userRatings.toSeq), sc.parallelize(userLikesAndNearstPearson._2.toSeq), sc.parallelize(userLikesAndNearstPearson._1.toSeq), model)
  }

  override def predict(model: NeurModel, query: Query): PredictedResult = {
    val uMap = model.userMap.collectAsMap()
    if (!uMap.contains(query.user)) {
      //该用户没有过评分记录，返回空值
      logger.warn(s"该用户没有过评分记录，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //1.获取当前要推荐用户的Pearson值最大的用户列表
    val userPearson = model.userNearestPearson.collectAsMap()
    if (!userPearson.contains(query.user)) {
      //该用户没有对应的Pearson相似用户
      logger.warn(s"该用户没有相似的用户，无法生成推荐！${query.user}")
      return PredictedResult(Array.empty)
    }

    //2.所有用户最喜欢的前N部电影
    val userLikes = model.userLikesBeyondMean.collectAsMap()

    //当前用户已经观看过的列表
    val sawItem = uMap(query.user).map(r => (r.item, r.rating)).toMap

    //存储结果的列表
    val pearsonResult = new mutable.HashMap[String, Double]()
    //与当前查询用户相似度最高的用户，其观看过的且查询用户未看过的电影列表。
    userPearson(query.user).foreach(r => {
      //r._1 //相似的userID
      // r._2 //相似度
      if (userLikes.contains(r._1)) {
        //r._1用户有最喜欢的电影记录
        userLikes(r._1).map(r1 => {
          //r1.item
          //r1.rating
          if (!sawItem.contains(r1.item)) {
            //当前用户未观看过的电影r1.item
            if (pearsonResult.contains(r1.item)) {
              //这是已经在推荐列表中
              pearsonResult.update(r1.item, pearsonResult(r1.item) + r1.rating * r._2)
            } else {
              pearsonResult.put(r1.item, r1.rating * r._2)
            }
          }
        })
      }
    })


    val neurModel = model.neurModel
    val filtedResult = pearsonResult.filter(re => {
      val arr = List(query.user.toFloat, re._1.toFloat).toArray
      neurModel.predict(Nd4j.create(arr))(0) == 1
    })


    //排序取TopN
    val preResult = filtedResult.map(r => (r._1, r._2)).toList.sortBy(_._2).reverse.take(query.num).map(r => (r._1, r._2))

    //归一化并加上权重
    val sum = preResult.map(r => r._2).sum
    val weight = 2.0
    val returnResult = pearsonResult.map(r => {
      ItemScore(r._1, r._2 / sum * weight)
    })

    //排序，返回结果
    PredictedResult(returnResult.toArray)
  }
}
