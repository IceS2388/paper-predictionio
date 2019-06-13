package org.example.recommendation

import org.apache.predictionio.controller.PAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.mllib.recommendation.ALSModel

import grizzled.slf4j.Logger

/**
  * seed:是可选参数，MLlib ALS算法用其生产随机值。没有指定，则使用当前系统时间,导致每次产生的结果不同。当用于测试的时候，
  * 可以指定一个固定的值，用于对比。
  * */
case class ALSAlgorithmParams(
  rank: Int,//topN
  numIterations: Int,//迭代次数
  lambda: Double,
  seed: Option[Long]) extends Params
/**
  * ALSAlgorithm包括机器学习算法，和设置的参数。指定预测模板如何被构建。
  *ap:ALSAlgorithmParams存储在engine.json文件中。PredictionIO 会自动加载这些参数。
  * */
class ALSAlgorithm(val ap: ALSAlgorithmParams)
  extends PAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  if (ap.numIterations > 30) {
    logger.warn(
      s"ALSAlgorithmParams.numIterations > 30, current: ${ap.numIterations}. " +
      s"There is a chance of running to StackOverflowException." +
      s"To remedy it, set lower numIterations or checkpoint parameters.")
  }

  /**
    * 这是训练的方法
    * 当输入：pio train时使用。
    * 常用于训练预测模型。
    * 需要映射String类型的Rating到MLlib的Int类型的Rating。
    * 首先，你能重命名MLlib的Rating成MLlibRating。
    * import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
    * 然后，使用双向map“BiMap.stringInt”来记录String和Integer的索引。
    * 最后，把每一个Rating变成MLlibRating。
    * 其他的参数： rank, iterations, lambda and seed等被指定在engine.json中。
    * PredictionIO 会自动存储返回的模板。
    * */
  override
  def train(sc: SparkContext, data: PreparedData): ALSModel = {



    // MLLib ALS cannot handle empty training data.
    require(!data.ratings.take(1).isEmpty,
      s"RDD[Rating] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")
    // Convert user and item String IDs to Int index for MLlib

    val userStringIntMap = BiMap.stringInt(data.ratings.map(_.user))//用户的ID号,变成索引号
    val itemStringIntMap = BiMap.stringInt(data.ratings.map(_.item))//物品的ID号,变成索引号
    val mllibRatings = data.ratings.map( r =>
      // MLlibRating requires integer index for user and item
      //MLlibRating 需要user和item的integer类型的索引
      MLlibRating(userStringIntMap(r.user), itemStringIntMap(r.item), r.rating)
    )

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // Set checkpoint directory
    // sc.setCheckpointDir("checkpoint")

    // If you only have one type of implicit event (Eg. "view" event only),
    // set implicitPrefs to true
    //注意：如果只有特定的一种类型，设置implicitPrefs为true
    val implicitPrefs = false
    //ALS算法
    val als = new ALS()
    //设置计算时并行的块
    als.setUserBlocks(-1)
    als.setProductBlocks(-1)
    //设置矩阵计算的范围，默认是10
    als.setRank(ap.rank)
    //设置迭代次数
    als.setIterations(ap.numIterations)
    //保留所有的特征，但是减少参数的大小（magnitude）
    //于这里的规范化项的最高次为2次项，因而也叫L2规范化。
    als.setLambda(ap.lambda)
    //设置是否隐式优先
    als.setImplicitPrefs(implicitPrefs)
    als.setAlpha(1.0)
    als.setSeed(seed)
    //If the checkpoint directory is not set in [[org.apache.spark.SparkContext]],
    als.setCheckpointInterval(10)
    //执行,返回MatrixFactorizationModel类型，但是该类型不支持持久化。
    val m = als.run(mllibRatings)

    //把MatrixFactorizationModel转变成可进行持久化的ALSModel。
    new ALSModel(
      rank = m.rank,
      userFeatures = m.userFeatures,
      productFeatures = m.productFeatures,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap)
  }

  /**
    * 这是响应查询的方法。
    * 当调用： http://localhost:8000/queries.json时，调用本方法。
    * ALSModel继承MatrixFactorizationModel，并实现了PersistentModel持久化接口。
    *
    * */
  override
  def predict(model: ALSModel, query: Query): PredictedResult = {

    // Convert String ID to Int index for Mllib
    //把String类型的ID转换成Mllib的Int类型的索引
    model.userStringIntMap.get(query.user).map { userInt =>
      // create inverse view of itemStringIntMap
      //创建把物品的索引转变成其ID的Map
      val itemIntStringMap = model.itemStringIntMap.inverse
      // recommendProducts() returns Array[MLlibRating], which uses item Int
      // index. Convert it to String ID for returning PredictedResult
      //recommendProducts()是MatrixFactorizationModel的方法，其返回Array[MLlibRating]类型的推荐结果。
      //其中包含item的索引，把其转换为String类型的itemID
      val itemScores = model.recommendProducts(userInt, query.num)
        .map (r => ItemScore(itemIntStringMap(r.product), r.rating))

      //PredictionIO passes the returned PredictedResult object to Serving.
      //PredictionIO会把PredictedResult传递给Serving
      PredictedResult(itemScores)
    }.getOrElse{
      logger.info(s"No prediction for unknown user ${query.user}.")
      PredictedResult(Array.empty)
    }
  }

  // This function is used by the evaluation module, where a batch of queries is sent to this engine
  // for evaluation purpose.
  /**
    * 本方法用于评估模块,以验证为目的的一批查询发送到引擎。
    * */
  override
  def batchPredict(model: ALSModel, queries: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = {

    val userIxQueries: RDD[(Int, (Long, Query))] = queries
    .map { case (ix, query) => {
      //TODO:ix是什么？
      // If user not found, then the index is -1
      val userIx = model.userStringIntMap.get(query.user).getOrElse(-1)
      (userIx, (ix, query))
    }}

    // Cross product of all valid users from the queries and products in the model.
    val usersProducts: RDD[(Int, Int)] = userIxQueries
      .keys
      //筛选用户ID对应的矩阵索引不等于-1的用户
      .filter(_ != -1)
      //笛卡尔积形式与item数据构成用户索引-物品索引的矩阵
      .cartesian(model.productFeatures.map(_._1))

    // Call mllib ALS's predict function.
    //调用mllib ALS的预测方法
    val ratings: RDD[MLlibRating] = model.predict(usersProducts)

    // The following code construct predicted results from mllib's ratings.
    // Not optimal implementation. Instead of groupBy, should use combineByKey with a PriorityQueue
    // 不是最佳的实现。可以用combineByKey和PriorityQueue来代替groupBy
    val userRatings: RDD[(Int, Iterable[MLlibRating])] = ratings.groupBy(_.user)

    userIxQueries.leftOuterJoin(userRatings)
    .map {
      // When there are ratings
      case (userIx, ((ix, query), Some(ratings))) => {
        //对评分进行排序
        val topItemScores: Array[ItemScore] = ratings
        .toArray
        .sortBy(_.rating)(Ordering.Double.reverse) // 注意: 从大到小排序
        .take(query.num)
        .map { rating => ItemScore(
          //转换：从索引到物品ID
          model.itemStringIntMap.inverse(rating.product),
          rating.rating) }

        (ix, PredictedResult(itemScores = topItemScores))
      }
      // When user doesn't exist in training data
        //当用户不存在于训练数据集中时
      case (userIx, ((ix, query), None)) => {
        require(userIx == -1)
        (ix, PredictedResult(itemScores = Array.empty))
      }
    }
  }
}
