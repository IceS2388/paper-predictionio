# 自定义混合推荐测试

## 一、参考文档

PredictionIO入门手册:
https://predictionio.apache.org/templates/recommendation/quickstart/.
<br>
热点问题：
http://predictionio.apache.org/resources/faq/#using-predictionio

## 二、系统架构
PredictionIO主要由...

## 三、常用操作命令
### 1.`pio-docker`相关命令
#### 1.1 启动
```shell
$ cd /root/predictionio/docker
$ docker-compose -f docker-compose.yml -f pgsql/docker-compose.base.yml -f pgsql/docker-compose.meta.yml -f pgsql/docker-compose.event.yml -f pgsql/docker-compose.model.yml up &
$ pio-docker status //若一切正常，应该看到`[INFO] [Management$] Your system is all ready to go.`
```
#### 1.2 下载对应的模板
```shell
$ cd templates/
$ git clone https://github.com/IceS2388/paper-predictionio.git
$ cd paper-predictionio
```
#### 1.3 下载对应的模板
```shell

```

###
### 步骤
1. 往Event Server中导入数据。
2. 在DataSource中对数据进行清洗。
3. 重点是去除用户的评分偏好。利用偏好系数=(用户的平均评分-最小评分)/(最大评分-最小评分)
4. 构建对应的新评分矩阵。

### 混合推荐
最终的结果必定是由多个不同推荐的方法合并而来。
可能的方法：
1. 协同过滤。
2. 看Spark ML中推荐的包下的方法(只有ALS是分解矩阵，然而矩阵中存储的是什么有程序员决定。例如：用户与用户的相似度，)。
3. 基于用户自身历史记录的推荐。(包含最近浏览的推荐)
4. 热点推荐。

### 多维度测试必须通过卡方验证数据的相关性

### 协同过滤时必须考虑矩阵分解
由于基于矩阵分解的协同过滤
算法会将原始的用户/物品评分矩阵分解为两个有用户特征组成的低维用户特征矩阵和
物品特征矩阵，该算法会对分解后的矩阵的新用户和新物品进行聚类找出 K 个最近邻，
根据 K 给最近邻求出该用户对物品的评分。
优化后的基于矩阵分解的协同过滤算法可以解决基于矩阵分解的协同过
滤算法的冷启动问题。
矩阵增量计算思路

### 实时举证更新

### 余弦过滤方法算法
可加入P因子不同用户之间的评分差异度对相似度进行调节，pearson算法剔除了个人的评分偏好很好的解决了这个问题。

### 补充知识点
1. 模型为什么要训练？
在ALS算法中，如果是实时利用数据训练模型，然后再推荐的话。会非常耗费时间。
优点:如果根据一定的数据，先训练好模型。在调用推荐方法时，直接使用该模型会节省时间。
缺点：这决定了其不是实时的推荐系统。



## Versions

### v1.1.0
在原有项目的基础上，添加Pearson相似度算法模块，并设置其Pearson系数的权重为1.5。

### v1.0.0

基于predictionio-template-recommender项目的基础上改进而来。
<br>
原项目地址：https://github.com/IceS2388/predictionio-template-recommender
