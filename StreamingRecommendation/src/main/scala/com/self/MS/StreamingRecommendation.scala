package com.self.MS

import java.sql.{DriverManager, ResultSet}
import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * created by Bonnie on 2021/4/25
 */
object StreamingRecommendation {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "kafka.topic" -> "fromKafkaLog"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommendation").setMaster(config("spark.cores"))
    //创建SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    //创建kafkastream
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop002:9092, hadoop002:9093, hadoop002:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "logFilter",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParams)
    )
    //获取评分流
    val ratingStream = kafkaDStream.map(crd => {
      val attr = crd.value().split("::")
      (attr(0).toInt, attr(1).toInt, attr(2).toInt, attr(3).toLong)
    })
    //    ratingStream.print()
    //简单实时推荐算法
    ratingStream.foreachRDD(rdd =>
      rdd.map{  case (userId, movieId, score, timestamp) =>
        println(">>>>>>>>>>")
        val t = updateToMysql(s"select count(*) count from results where movie_id='$movieId'", 1)
        val time = new Date().getTime
        if( t.next() ){
          if( t.getInt("count")==0 ){
            updateToMysql(s"insert into results value('$time', '$movieId', '$score', 1)", 2)
            println("insert successfully!!")
          }else {
            val u = updateToMysql(s"select avg_score, num from results where movie_Id='$movieId'", 1)
            var s = 0
            var n = 0
            while( u.next() ){
              val avg_score = u.getObject("avg_score").toString.toInt
              val nums = u.getByte("num").toInt
              s += avg_score*nums
              n += nums
            }
            s += score
            n += 1
            s = s/n
            updateToMysql(s"delete from results where movie_Id='$movieId'", 2)
            updateToMysql(s"insert into results value('$time', '$movieId', '$s', '$n')", 2)
            println("update successfully!!")

          }
          updateToMysql("drop table if exists top5Results", 2)
          updateToMysql("create table top5Results select movie_id, avg_score, num from results order by avg_score desc limit 5", 2)
          println("update top5Results successfully!!")
        }
      }.count()
    )

    ssc.start()
    println("streaming start collecting data!!!")
    ssc.awaitTermination()
    //    ssc.stop(true)
  }

//  def getValues(tablename: String, sparkSession: SparkSession): DataFrame ={
//    val map: Map[String, String] = Map[String, String](
//      elems = "url" -> "jdbc:mysql://192.168.186.100:3306/movie_information",
//      "driver" -> "com.mysql.jdbc.Driver",
//      "user" -> "root",
//      "password" -> "niit1234",
//      "dbtable" -> tablename
//    )
//    val score = sparkSession.read.format("jdbc").options(map).load
//    score
//  }

  //n: 1 - executeQuery, 2 - executeUpdate
  def updateToMysql(sql: String, n: Int): ResultSet = {
    val url = "jdbc:mysql://hadoop002:3306/movie_information?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "niit1234"

    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    if(n==1){
      val res = statement.executeQuery(sql)
      res
    }else {
      statement.executeUpdate(sql)
      null
    }
  }
}
