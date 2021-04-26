## 1. 数据初始化
### 初始化数据
（1）启动Linux，并创建`${DATA_DIR}`目录来存储文件；  
（2）将`MoviesSystem/files`下的`getLog.sh`、`movies.dat`、`rating.dat`文件存储到`${DATA_DIR}`目录下；  
（3）在`${DATA_DIR}`目录下创建`agent.log`文件，具体命令如下：
```$xslt
touch agent.log
```
### 参数内容修改
```
（1）将`${DATA_DIR}`替换为具体的绝对路径；
```

## 2. 启动 getLog.sh、 zookeeper、 flume、 kafka
### 启动应用
（1）进入`${DATA_DIR}`目录；
```$xslt
cd ${DATA_DIR}
```
（2）启动`getLog.sh`:
```$xslt
./getLog.sh
```
（3）启动`Zookeeper`
```$xslt
# 进入zookeeper的conf目录
cd ${ZOOKEEPER_HOME}/conf
# 执行下述命令
zkServer.sh start zoo1.cfg
zkServer.sh start zoo2.cfg
zkServer.sh start zoo3.cfg
```
（4）启动`kafka`
```$xslt
# 进入kafka的config目录
cd ${KAFKA_HOME}/conf
# 执行下述命令
kafka-server-start.sh -daemon server-1.properties
kafka-server-start.sh -daemon server-2.properties
kafka-server-start.sh -daemon server-3.properties
```
（5）运行flume文件
```$xslt
# 进入flume目录
cd ${FLUME_HOME}
# 执行下述命令
flume-ng agent -c conf/ -f conf/log2kafka.properties -n a1 -Dflume.root.logger=INFO,console
```
### 参数内容修改
（1）进入`flume`目录
```$xslt
cd ${FLUME_HOME}
```
（2）修改`conf/log2kafka.properties`文件
```$xslt
# 第一处修改：${DATA_DIR}/agent.log
a1.sources.r1.command = tail -F ${DATA_DIR}/agent.log

# 第二处修改：${KAFKA_TOPIC}为要导入的kafka topic名称
a1.sinks.k1.kafka.topic = ${KAFKA_TOPIC}

# 第三处修改：${BOOTSTRAP_SERVER}为kafka的bootstrap server
a1.sinks.k1.kafka.bootstrap.servers = ${BOOTSTRAP_SERVER}
```

## 3. 配置MySQL
（1）进入`mysql`
```$xslt
mysql -uroot -p
```
输入密码后成功登录；
（2）创建数据库
```$xslt
create database movie_information;
use movie_information;
```
（3）创建表
```$xslt
# 创建 movies 表
create table movies(
id varchar(5) primary key not null,
name varchar(100) not null,
tags varchar(100) not null
);

# 创建 results 表
create table movies(
timestamp varchar(20) primary key not null,
movie_id varchar(5) not null,
avg_score varchar(5) not null,
num int(5) not null
);

# 创建 top5Results 表
create table top5Results(
movie_id varchar(5) not null,
avg_score varchar(5) not null,
num int(5) not null
);
```

## 4. 启动 MoviesSystem 下的 StreamingRecommendation module
### 启动 StreamingRecommendation.scala
点击 main 左侧的绿色三角形；
### 修改参数内容
```$xslt
# 第一处修改：${KAFKA_TOPIC}为要导入的kafka topic名称
"kafka.topic" -> ${KAFKA_TOPIC}

# 第二处修改：${BOOTSTRAP_SERVER}为kafka的bootstrap server
"bootstrap.servers" -> ${BOOTSTRAP_SERVER},

# 第三处修改：${TOPIC_GROUP}为${KAFKA_TOPIC}所属的组名
"group.id" -> ${TOPIC_GROUP},

# 第四处修改：${HOSTNAME}为主机名，${MYSQL_ROOT}为mysql登录名称，${MYSQL_PASSWORD}为mysql登陆密码
val url = "jdbc:mysql://${HOSTNAME}:3306/movie_information?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC"
val driver = "com.mysql.jdbc.Driver"
val username = ${MYSQL_ROOT}
val password = ${MYSQL_PASSWORD}
```

## 5. 启动 MoviesSystem 下的 showing module
### 启动 ShowingApplication.java
（1）点击 main 左侧的绿色三角形；
（2）打开`chrome`浏览器，输入`localhost:8080/index`；
（3）成功运行，隔一段时间刷新一次页面，排行榜会根据不断获取的结果做动态修改；
### 修改参数内容
```$xslt
# 修改 application.yml 下的内容：${HOSTNAME}为主机名，${MYSQL_ROOT}为mysql登录名称，${MYSQL_PASSWORD}为mysql登陆密码
spring:
  datasource:
    url: jdbc:mysql:///${HOSTNAME}:3306/movie_information?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC
    username: ${MYSQL_ROOT}
    password: ${MYSQL_PASSWORD}
    driver-class-name: com.mysql.jdbc.Driver
```

至此，就可以成功实现实时获取数据进行计算。