package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author WithQ
 * @create 2020-11-25 19:30
 *         根据黑名单过滤后的数据集，统计每天各大区各个城市广告点击总数并保存至MySQL中
 */
object DateAreaCityAdCountHandler {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    val dateAreaCityAdidCount_count: DStream[((String, String, String, String), Int)] = filterAdsLogDStream.map(
      ads_log =>
        ((sdf.format(new Date(ads_log.timestamp)), ads_log.area, ads_log.city, ads_log.adid), 1)
    ).reduceByKey(_ + _)

    dateAreaCityAdidCount_count.foreachRDD(
      rdd =>
        rdd.foreachPartition{
          iterator => {
            val connection: Connection = JDBCUtil.getConnection
            iterator.foreach{
              case((date,area,city,adid),count) =>
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |insert into area_city_ad_count
                    |(dt,area,city,adid,count)
                    |values (?,?,?,?,?)
                    |on duplicate key
                    |update count = count + ?
                    |""".stripMargin,
                  Array(date,area,city,adid,count,count)
                )
            }
            connection.close()
          }
        }
    )

  }

}
