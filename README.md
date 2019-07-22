# 自定义混合推荐测试

## 一、参考文档
PredictionIO入门手册:
https://predictionio.apache.org/templates/recommendation/quickstart/.
<br>
热点问题：
http://predictionio.apache.org/resources/faq/#using-predictionio

## 二、常用操作命令
### 2.1 `pio-docker`相关命令
**启动**
```shell
$ cd /root/predictionio/docker
$ docker-compose -f docker-compose.yml -f pgsql/docker-compose.base.yml -f pgsql/docker-compose.meta.yml -f pgsql/docker-compose.event.yml -f pgsql/docker-compose.model.yml up &
$ pio-docker status //若一切正常，应该看到`[INFO] [Management$] Your system is all ready to go.`
```
**下载对应的模板 或者 更新模板**
* 下载
```shell
$ cd templates/
$ git clone https://github.com/IceS2388/paper-predictionio.git
$ cd paper-predictionio
```
* 更新
```shell
$ cd templates/paper-predictionio
$ git pull origin develop
```
**生成模板**
```shell
$ sbt clean assembly package
```
**训练模板**
```shell
$ pio-docker train  -- --driver-memory 4g --executor-memory 5g --verbose
```
**部署模板**
```shell
$ pio-docker deploy
```
**评估模板**
```shell
$ pio-docker eval org.example.recommendation.RecommendationEvaluation org.example.recommendation.EngineParamsList -- --driver-memory 3g --executor-memory 5g --verbose
```
**运行结果：**报错`OOM`需要阅读Spark相关文档后继续。或者删除临时文件夹中prediction相关的文件。

## 三、系统架构
PredictionIO主要由DASE4个组件组成。
### 3.1 [D] Data Source and Data Preparator 数据源和数据准备器
Data Source从事件源读取数据，并转变成指定的格式。Data Preparator处理数据源传来的数据，使其转变为适合特定算法的数据结构。
### 3.2 [A] Algorithm 算法
算法组件包含机器学习的算法和对应的设置参数，决定如何构建预测模型。
### 3.3 [S] Serving
服务组件接受预测查询，返回预测的结果。若引擎有多个算法，Serving会整合所有算法的预测结果为一个。此外，在此处可添加业务处理逻辑，定制最后的推荐结果。
### 3.4 [E]Evaluation Metrics 评估指标
评估指标用数值来量化预测精度。可以用作算法比较和调整算法参数的指标。
![A PredictionIO Engine Instance](http://predictionio.apache.org/images/engineinstance-overview-c6668657.png)
### 3.5 引擎使用规则
引擎的主要功能：
* 使用训练数据训练模型，可以被作为WebService服务部署。
* 实时响应预测查询。
使用`DASE`组件的引擎在部署时必须指定：
* 一个数据源。
* 一个数据准备器。
* 一个或多个数据算法。
* 一个Serving。
每个引擎各自独立处理数据和构建预测模型。因此，每个引擎的Serves只处理自己的预测结果。例如：你可为你的移动app部署两个引擎，一个为用户推荐新闻；另一个为用户建议新朋友。

### 3.6 训练模型流程-从DASE视角
当你运行`pio train`时，如下的图展示了DASE工作流。
![训练图](http://predictionio.apache.org/images/engine-training-93bc1b69.png)

### 3.7 相应查询流程-从DASE视角
当引擎收到`REST`查询后的响应流程图如下：
![响应流程图](http://predictionio.apache.org/images/engine-query-8d7311ff.png)

## 四、添加自定义算法的过程
### 4.1 添加对应的文件
* 算法文件必须以Algorithm.scala结尾。(必须实现指定的`trait`)
* 模板文件必须以Model.scala结尾。(必须实现指定的`trait`)
### 4.2 修改`Engine.scala`文件
```scala
object RecommendationEngine extends EngineFactory {
  def apply() = {
    /**
      *控制使用的推荐算法
      * */
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map(
        "als" -> classOf[ALSAlgorithm],
        "pus" -> classOf[PUSAlgorithm],
        "mv" -> classOf[MViewAlgorithm]),  //<-这行是新添加的，这样Engine在部署后，会调用指定的算法。
      classOf[Serving])
  }
}
```
### 4.3 修改`engine.json`文件
`engine.json`文件主要存放各个算法的参数。在`algorithms`节点中添加新的参数，如下所示：
```json
"algorithms": [
    ...
    ,{
      "name": "mv",
      "params": {
        "maxItems": 300
      }
    }
```
### 4.4 实现算法和模型
* ALS 最小交替乘二法
* MV 最热门的排序算法
* Pearson和随机森林算法
* Pearson和神经网络算法
...
### 4.5 修改`Serving.scala`文件，集成各个算法的推荐结果
```scala
 val result=new mutable.HashMap[String,Double]()

    predictedResults.map(pr=>{
      pr.itemScores.map(is=>{
        if(!result.contains(is.item)){
          result.put(is.item,is.score)
        }else{
          val oldScore=result.get(is.item).get
          result.update(is.item,oldScore+is.score)
        }
      })
    })


    PredictedResult(result.map(r=>new ItemScore(r._1,r._2)).toArray.sortBy(_.score).reverse.take(query.num))
```

## 五、评估介绍
具体更改后的评估，请看`Evaluation.scala`文件。
## 六、补充知识点
### 6.1 模型为什么要训练？
在ALS算法中，如果是实时利用数据训练模型，然后再推荐的话。会非常耗费时间。
优点:如果根据一定的数据，先训练好模型。在调用推荐方法时，直接使用该模型会节省时间。
缺点：这决定了其不是实时的推荐系统。

## 七、思想碎片
### 7.1 主要流程
1. 往Event Server中导入数据。
2. 在DataSource中对数据进行清洗。
3. 重点是去除用户的评分偏好。利用偏好系数=(用户的平均评分-最小评分)/(最大评分-最小评分)
4. 构建对应的新评分矩阵。

### 7.2 混合推荐
最终的结果必定是由多个不同推荐的方法合并而来。
可能的方法：
1. 协同过滤(Pearson已实现)。
2. 看Spark ML中推荐的包下的方法(ALS分解矩阵，效果不理想)。
3. 基于用户自身历史记录的推荐。(包含最近浏览的推荐,待实现)
4. 热点推荐。

### 7.3 多维度测试必须通过卡方验证数据的相关性(多维度需要经过特征工程，选取主要指标。)

### 7.4 协同过滤时必须考虑矩阵分解
由于基于矩阵分解的协同过滤
算法会将原始的用户/物品评分矩阵分解为两个有用户特征组成的低维用户特征矩阵和
物品特征矩阵，该算法会对分解后的矩阵的新用户和新物品进行聚类找出 K 个最近邻，
根据 K 给最近邻求出该用户对物品的评分。
优化后的基于矩阵分解的协同过滤算法可以解决基于矩阵分解的协同过
滤算法的冷启动问题。
矩阵增量计算思路

### 7.5 实时矩阵更新

### 7.6 余弦过滤方法算法
可加入P因子不同用户之间的评分差异度对相似度进行调节，pearson算法剔除了个人的评分偏好很好的解决了这个问题。

### 7.7 去除用户偏好的另外一种方法
1. 标准线:5/2=2.5。
2. 求出每个用户评价的平均分。
3. 以2.5为基础，采用新评分= 旧评分+(2.5-用户的平均值)
4. 再采用ALS算法进行运算。
**结果：** 比原先的准确度下降了一半。




## Versions

### v1.5.0
* 对Pearson推荐的结果进行了贝叶斯分类筛选。

### v1.4.0
* 对Pearson推荐的结果进行了随机森林的分类筛选。

### v1.3.0
* 修改Pearson推荐结果所占比重。

### v1.2.0
* 增加访问量最大推荐。

### v1.1.0
* 在原有项目的基础上，添加Pearson相似度算法模块，并设置其Pearson系数的权重为1.5。

### v1.0.0
* 基于[predictionio-template-recommender](https://github.com/IceS2388/predictionio-template-recommender)项目的基础上改进而来。
