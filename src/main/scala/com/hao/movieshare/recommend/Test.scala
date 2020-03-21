package com.hao.movieshare.recommend

import java.sql.Timestamp

import org.apache.spark.sql.{Row, SparkSession}
import play.api.libs.json.{JsValue, Json}

object Test {
  def main(args: Array[String]): Unit = {
    if (args.length<2){
      return
    }
    val startDate=args(0)
    val endDate=args(1)
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val jdbcFrame=sparkSession.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/movie_share?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=GMT%2B8")
      .option("user","root").option("password","123456").option("dbtable","movie_share.log")
      .option("driver","com.mysql.cj.jdbc.Driver").load()

    jdbcFrame.createTempView("log")

    import sparkSession.implicits._
    val filterDateDateFrame=sparkSession.sql("select " +
      "user_id,username,business_type,method,params,create_time from log where create_time between '"+startDate+"' and '"+endDate+"' "+
      "and log_type='INFO' and business_type in ('play_video','favorite_video','comment_video' )"
    )

    val paramsDateFrame=filterDateDateFrame.map(row=>{
      val json: JsValue=Json.parse(row.getAs[String]("params"))
      val businessType = row.getAs[String]("business_type")
      var videoId=""
      businessType match {
        case "play_video"=>videoId=json("videoId").toString()
        case "comment_video"=>videoId=json("videoComment")("videoId").toString()
        case "favorite_video"=>videoId=json("favoriteVideo")("videoId").toString()
      }

      LogItem(row.getAs[Int]("user_id"),row.getAs[String]("username"),
        videoId,row.getAs[String]("business_type"),row.getAs[Timestamp]("create_time"))
    })

    paramsDateFrame.createTempView("parse_log")

    val playVideoWeight=0.2
    val favoriteVideoWeight=0.4
    val commentVideoWeight=0.4

    val videoFrame=sparkSession.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/movie_share?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=GMT%2B8")
      .option("user","root").option("password","123456").option("dbtable","movie_share.video")
      .option("driver","com.mysql.cj.jdbc.Driver").load()

    videoFrame.createTempView("video")

    val groupLog = sparkSession.sql("select " +
      " sum( case " +
      " when businessType='play_video' then times*" +playVideoWeight+
      " when businessType='favorite_video' then times*" +favoriteVideoWeight+
      " when businessType='comment_video' then times*" + commentVideoWeight+
      " end) as score,videoId from (select count(*) as times, videoId, businessType from parse_log group by videoId, businessType) as tmp group by videoId")


    groupLog.createTempView("group_log")

    val scoreLog=sparkSession.sql("select (score*0.4+video_rate*0.6) as score,videoId,category_id as categoryId from group_log join video where group_log.videoId=video.video_id order by score desc")

    scoreLog.show()

    sparkSession.close()
  }
}

case class LogItem(userId:Int,userName:String,videoId:String,businessType:String,createTime:Timestamp)