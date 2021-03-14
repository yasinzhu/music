package cn.yasin.scala.musicproject.dm.content

import cn.yasin.scala.musicproject.common.ConfigUtils
import org.apache.spark.sql.SparkSession

import java.util.Properties

object GenerateTmSongRsiD {
    private val localRun: Boolean = ConfigUtils.LOCAL_RUN
    private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
    private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
    private val mysqlUrl: String = ConfigUtils.MYSQL_URL
    private val mysqlUser: String = ConfigUtils.MYSQL_USER
    private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD
    var sparkSession : SparkSession = _
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println(s"需要指定数据日期,格式为(YYYYMMDD)")
            System.exit(1)
        }
        if (localRun == true) {
            sparkSession = SparkSession.builder().appName("GenerateTmSongRsiD").master("local")
                .config("hive.metastore.uris",hiveMetastoreUris).enableHiveSupport().getOrCreate()
        } else {
            sparkSession = SparkSession.builder().appName("GenerateTmSongRsiD")
                .config("hive.metastore.uris",hiveMetastoreUris).enableHiveSupport().getOrCreate()
        }
        val currentDate = args(0)
        sparkSession.sql(s"use ${hiveDatabase}")
        val dataFrame = sparkSession.sql(
            s"""
               |select
               |   data_dt,                  --日期
               |   NBR,                      --歌曲ID
               |   NAME,                     --歌曲名称
               |   SING_CNT,                 --当日点唱量
               |   SUPP_CNT,                 --当日点赞量
               |   RCT_7_SING_CNT,           --近七天点唱量
               |   RCT_7_SUPP_CNT,           --近七天点赞量
               |   RCT_7_TOP_SING_CNT,       --近七天最高日点唱量
               |   RCT_7_TOP_SUPP_CNT,       --近七天最高日点赞量
               |   RCT_30_SING_CNT,          --近三十天点唱量
               |   RCT_30_SUPP_CNT,          --近三十天点赞量
               |   RCT_30_TOP_SING_CNT,      --近三十天最高日点唱量
               |   RCT_30_TOP_SUPP_CNT       --近三十天最高日点赞量
               | from TW_SONG_FTUR_D
               | where data_dt = ${currentDate}
               |""".stripMargin)
        import org.apache.spark.sql.functions._
        dataFrame.withColumn("RSI_1D",pow(
            log(col("SING_cnt") / 1 + 1) * 0.63 * 0.8 + log(col("SUPP_CNT") / 1 + 1) * 0.63 * 0.2,2) * 10)
            .withColumn("RSI_7D",pow(
                (log(col("RCT_7_SING_CNT") / 7 + 1) * 0.63 + log(col("RCT_7_TOP_SING_CNT") + 1) * 0.37) * 0.8
                    +
                (log(col("RCT_7_SUPP_CNT") / 7 + 1) * 0.63 + log(col("RCT_7_TOP_SUPP_CNT") + 1) * 0.37) * 0.2,2) * 10)
            .withColumn("RSI_30D",pow(
                (log(col("RCT_30_SING_CNT") / 30 + 1 ) * 0.63 + log(col("RCT_30_TOP_SING_CNT") + 1) * 0.37) * 0.8
                    +
                (log(col("RCT_30_SUPP_CNT") / 30 + 1) * 0.63 + log(col("RCT_30_TOP_SUPP_CNT") + 1 ) * 0.37) * 0.2,2)*10)
            .createTempView("TEMP_TW_SONG_FTUR_D")
        val rsi_1d = sparkSession.sql(
            s"""
               | select
               |  "1" as PERIOD,NBR,NAME,RSI_1D as RSI,
               |  row_number() over(partition by data_dt order by RSI_1D desc) as RSI_RANK
               | from TEMP_TW_SONG_FTUR_D
               | """.stripMargin)
        val rsi_7d = sparkSession.sql(
            s"""
               | select
               |  "7" as PERIOD,NBR,NAME,RSI_7D as RSI,
               |  row_number() over(partition by data_dt order by RSI_7D desc) as RSI_RANK
               | from TEMP_TW_SONG_FTUR_D
               | """.stripMargin)
        val rsi_30d = sparkSession.sql(
            s"""
               | select
               |  "30" as PERIOD,NBR,NAME,RSI_30D as RSI,
               |  row_number() over(partition by data_dt order by RSI_30D desc) as RSI_RANK
               | from TEMP_TW_SONG_FTUR_D
               | """.stripMargin)
        rsi_1d.union(rsi_7d).union(rsi_30d).createTempView("result")
        sparkSession.sql(
            s"""
               |insert overwrite table TW_SONG_RSI_D partition(data_dt=${currentDate}) select * from result
               |""".stripMargin)
        val properties = new Properties()
        properties.setProperty("user",mysqlUser)
        properties.setProperty("password",mysqlPassword)
        properties.setProperty("driver","com.mysql.jdbc.Driver")
        sparkSession.sql(
            s"""
               |select ${currentDate} as date_dt,PERIOD,NBR,NAME,RSI,RSI_RANK from result where rsi_rank <= 30
               |""".stripMargin).write.mode(saveMode = "overwrite").jdbc(mysqlUrl,"tm_song_rsi",properties)
        println("****** All Finished ******")
    }
}
