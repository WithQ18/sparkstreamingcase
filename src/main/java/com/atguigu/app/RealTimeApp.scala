package com.atguigu.app

import java.util.Properties

import com.atguigu.handler.{BlackListHandler, DateAreaCityAdCountHandler}
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author WithQ
 * @create 2020-11-25 14:14
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp ").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //获取配置文件的信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    //从Kafka中读取数据，并将数据包装为Ads_log
    val kfInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(properties.getProperty("kafka.topic"),ssc)
    val inputDStream: DStream[Ads_log] = kfInputDStream.map(
      record => {
        val value: String = record.value()
        val log: Array[String] = value.split(" ")
        Ads_log(log(0).toLong, log(1), log(2), log(3), log(4))
      }
    )
    
    //需求一：根据MySQL中的黑名单过滤当前数据集
    val filteredDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(inputDStream)
    //需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filteredDStream)

    //测试打印
    filteredDStream.cache()
    filteredDStream.count().print()

    //7.需求二：统计每天各大区各个城市广告点击总数并保存至MySQL中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filteredDStream)

    ssc.start()
    ssc.awaitTermination()


  }
}


// 时间 地区 城市 用户id 广告id
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
