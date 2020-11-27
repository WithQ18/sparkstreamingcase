package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat

import java.util.Date
import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author WithQ
 * @create 2020-11-25 14:37
 *         日志数据统计并将每日点击广告超过30次的用户添加到黑名单
 */
object BlackListHandler {
  def addBlackList(inputDStream: DStream[Ads_log]): Unit = {
    //1.转换日志格式并统计单日每个用户点击每个广告的总次数
    val date_user_ad_count: DStream[((String, String, String), Long)] = inputDStream.map(
      ads_log => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        //转换一条日志为(yyyy-MM-dd格式的时间, userid, adid), 1)的格式 1为单击次数
        ((sdf.format(new Date(ads_log.timestamp)), ads_log.userid, ads_log.adid), 1L)
      }
    ).reduceByKey(_ + _) //统计总单击次数


    date_user_ad_count.foreachRDD(
      rdd => rdd.foreachPartition(
        iterator => iterator.foreach {
          case ((date, userid, adid), count) =>

            //2.将统计结果写入到统计结果的数据库
            val connection: Connection = JDBCUtil.getConnection
            JDBCUtil.executeUpdate(
              connection,
              """
                |insert into user_ad_count
                |(dt,userid,adid,count)
                |values (?,?,?,?)
                |on duplicate key
                |update count = count + ?
                |""".stripMargin,
              Array(date, userid, adid, count, count)
            )

            //3.查找点击次数>30次的用户，加入黑名单
            val ct: Long = JDBCUtil.getDataFromMysql(
              connection,
              """
                |select count from user_ad_count
                |where dt=? and userid=? and adid =?
                |""".stripMargin,
              Array(date, userid, adid)
            )
            if (ct > 30){
              JDBCUtil.executeUpdate(
                connection,
                """
                  |insert into black_list
                  |(userid)
                  |values (?)
                  |on duplicate key
                  |update userid = ?
                  |""".stripMargin,
                Array(userid,userid)
              )
            }

            connection.close()
        }
      )
    )
  }

  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] ={
    adsLogDStream.filter(
      ads_log => {
        val connection: Connection = JDBCUtil.getConnection
        val bool: Boolean = JDBCUtil.isExist(
          connection,
          """
            |select * from black_list where userid = ?
            |""".stripMargin,
          Array(ads_log.userid)
        )

        connection.close()

        !bool
      }
    )
  }

}
