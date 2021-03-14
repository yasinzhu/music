package cn.yasin.scala.musicproject.dm.machine

import cn.yasin.scala.musicproject.common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTmMacRegionStatD {
    val localRun: Boolean = ConfigUtils.LOCAL_RUN
    val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS

    val hiveDataBase = ConfigUtils.HIVE_DATABASE
    var sparkSession: SparkSession = _

    private val mysqlUrl = ConfigUtils.MYSQL_URL
    private val mysqlUser = ConfigUtils.MYSQL_USER
    private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

    def main(args: Array[String]): Unit = {
        if (localRun) {
            sparkSession = SparkSession.builder().master("local")
                .config("hive.metastore.uris", hiveMetaStoreUris)
                .config("spark.sql.shuffle.partitions", 10)
                .enableHiveSupport().getOrCreate()
        } else {
            sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions", 10).enableHiveSupport().getOrCreate()
        }

        if (args.length < 1) {
            println(s"请输入数据日期,格式例如：年月日(20201231)")
            System.exit(1)
        }
        val analyticDate = args(0)

        sparkSession.sparkContext.setLogLevel("Error")
        sparkSession.sql(s"use ${hiveDataBase}")

        sparkSession.sql(
            s"""
               | SELECT
               |   PRVC,   --省份
               |   CTY,    --城市
               |   COUNT(MID) AS MAC_CNT, --机器数量
               |   CAST(SUM(TOT_REV) AS DECIMAL(10,4)) AS MAC_REV,  --总营收
               |   CAST(SUM(TOT_REF) AS DECIMAL(10,4)) AS MAC_REF,  --总退款
               |   SUM(REV_ORDR_CNT) AS MAC_REV_ORDR_CNT,  --总营收订单数
               |   SUM(REF_ORDR_CNT) AS MAC_REF_ORDR_CNT,  --总退款订单数
               |   SUM(CNSM_USR_CNT) AS MAC_CNSM_USR_CNT,  --总消费用户数
               |   SUM(REF_USR_CNT) AS MAC_REF_USR_CNT  --总退款用户数
               | FROM TW_MAC_STAT_D
               | WHERE DATA_DT = ${analyticDate}
               | GROUP BY PRVC,CTY
      """.stripMargin).createTempView("TEMP_MAC_REGION_STAT")

        sparkSession.sql(
            s"""
               | insert overwrite table TM_MAC_REGION_STAT_D partition (data_dt=${analyticDate}) select * from TEMP_MAC_REGION_STAT
      """.stripMargin)

        val properties = new Properties()
        properties.setProperty("user", mysqlUser)
        properties.setProperty("password", mysqlPassword)
        properties.setProperty("driver", "com.mysql.jdbc.Driver")
        sparkSession.sql(
            s"""
               | select ${analyticDate} as data_dt ,* from TEMP_MAC_REGION_STAT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_mac_region_stat_d", properties)
        println("****** All Finished ******")
    }
}
