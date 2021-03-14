package cn.yasin.scala.musicproject.dm.content

import cn.yasin.scala.musicproject.common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTmUsrMrchntStatD {
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
        sparkSession.sql(s"use $hiveDataBase ")

        sparkSession.sql(
            s"""
               | select
               |   AGE_ID AS ADMIN_ID,   --代理人
               |   PAY_TYPE,
               |   SUM(REV_ORDR_CNT) AS REV_ORDR_CNT,  --总营收订单数
               |   SUM(REF_ORDR_CNT) AS REF_ORDR_CNT,  --总退款订单数
               |   CAST(SUM(TOT_REV) AS Double) AS TOT_REV,  --总营收
               |   CAST(SUM(TOT_REF) AS Double) AS TOT_REF,  --总退款
               |   CAST(SUM(TOT_REV * NVL(INV_RATE,0)) AS DECIMAL(10,4)) AS TOT_INV_REV,  --投资人营收
               |   CAST(SUM(TOT_REV * NVL(AGE_RATE,0)) AS DECIMAL(10,4)) AS TOT_AGE_REV,  --代理人营收
               |   CAST(SUM(TOT_REV * NVL(COM_RATE,0)) AS DECIMAL(10,4)) AS TOT_COM_REV,  --公司营收
               |   CAST(SUM(TOT_REV * NVL(PAR_RATE,0)) AS DECIMAL(10,4)) AS TOT_PAR_REV    --合伙人营收
               | from TW_MAC_STAT_D
               | WHERE DATA_DT = ${analyticDate}
               | GROUP BY AGE_ID,PAY_TYPE
      """.stripMargin).createTempView("TEMP_USR_MRCHNT_STAT")

        sparkSession.sql(
            s"""
               | insert overwrite table TM_USR_MRCHNT_STAT_D partition (data_dt=${analyticDate}) select * from TEMP_USR_MRCHNT_STAT
      """.stripMargin)

        val properties = new Properties()
        properties.setProperty("user", mysqlUser)
        properties.setProperty("password", mysqlPassword)
        properties.setProperty("driver", "com.mysql.jdbc.Driver")
        sparkSession.sql(
            s"""
               | select ${analyticDate} as data_dt ,* from TEMP_USR_MRCHNT_STAT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tm_usr_mrchnt_stat_d", properties)
        println("****** All Finished ******")
    }
}
