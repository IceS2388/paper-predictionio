[TOC]
# PredictionIO手动启动和停止
## 1.手动启动
### 1.1 启动Hadoop
```
HADOOP_PATH="${HADOOP_HOME}/sbin"
${HADOOP_PATH}/start-dfs.sh
${HADOOP_PATH}/mr-jobhistory-daemon.sh start historyserver
${HADOOP_PATH}/start-yarn.sh
```
### 1.2 启动Spark
```
${SPARK_HOME}/sbin/start-all.sh
```
### 1.3 启动HBase
```
${HBASE_HOME}/bin/start-hbase.sh
```
### 1.4 启动Elasticsearch
```
${EL_HOME}/bin/elasticsearch -d &
```
### 1.5 启动Eventserver

```
pio eventserver &
```
### 1.6 检查
```
pio status
```
### 1.7 构建和部署
```
# only if new config or code version needs to be updated
pio build
# only if the model is to be updated
pio train
#部署
pio deploy
```

## 2.手动停止
### 2.1 停止pio deploy
```
ps -aux | grep deploy
kill <deploy-pid>
```
### 2.2 停止EventServer
```
ps -aux | grep eventserver
kill <eventserver-pid>
```
### 2.3 Note: This needs to be done on all machines that run Elasticsearch. Find Elasticsearch server process with:
```
ps -aux | grep lasticsearch
kill <elasticsearch-pid>
```
### 2.4 停止HBase
```
${HBASE_HOME}/bin/stop-hbase.sh
```
### 2.5 停止Spark
```

${SPARK_HOME}/sbin/stop-all.sh
```
### 2.6 停止Hadoop
```
HADOOP_PATH="${HADOOP_HOME}/sbin"

${HADOOP_PATH}/yarn-daemon.sh stop resourcemanager
${HADOOP_PATH}/stop-yarn.sh
${HADOOP_PATH}/mr-jobhistory-daemon.sh stop historyserver
${HADOOP_PATH}/stop-dfs.sh
```