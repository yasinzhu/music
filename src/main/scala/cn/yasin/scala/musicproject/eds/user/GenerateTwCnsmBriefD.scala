package cn.yasin.scala.musicproject.eds.user

import cn.yasin.scala.musicproject.common.ConfigUtils
import cn.yasin.scala.musicproject.eds.machine.GenerateTwMacStatD.{mysqlPassword, mysqlUrl, mysqlUser, sparkSession}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

/**
  * 由 ODS层 TO_YCAK_CNSM_D 机器消费订单明细表  生成 EDS 层 TW_CNSM_BRIEF_D 消费订单流水日增量表
  */
object GenerateTwCnsmBriefD {
  val localRun : Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBase = ConfigUtils.HIVE_DATABASE
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD
  var sparkSession : SparkSession = _

  def main(args: Array[String]): Unit = {
    if(localRun){//本地运行
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris",hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions",10)
        .enableHiveSupport().getOrCreate()
    }else{//集群运行
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions",10).enableHiveSupport().getOrCreate()
    }

    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }

    val analyticDate = args(0)
    sparkSession.sql(s"use $hiveDataBase ")
    sparkSession.sparkContext.setLogLevel("Error")
    sparkSession.sql(
      s"""select
        | ID,      --ID
        | TRD_ID,   --第三方交易编号
        | cast(UID as string) AS UID, --用户ID
        | MID,                --机器ID
        | PRDCD_TYPE,         --产品类型
        | PAY_TYPE,           --支付类型
        | ACT_TM,             --消费时间
        | PKG_ID,             --套餐ID
        | case when AMT<0 then AMT*-1 else AMT end AS COIN_PRC,    --币值
        | 1 AS COIN_CNT,      --币数 ，单位分
        | ACT_TM as UPDATE_TM,  --状态更新时间
        | ORDR_ID,      --订单ID
        | ACTV_NM,      --优惠活动名称
        | PKG_PRC,      --套餐原价
        | PKG_DSCNT,    --套餐优惠价
        | CPN_TYPE,      --优惠券类型
        | CASE WHEN ORDR_TYPE = 1 THEN 0
        |      WHEN ORDR_TYPE = 2 THEN 1
        |      WHEN ORDR_TYPE = 3 THEN 2
        |	      WHEN ORDR_TYPE = 4 THEN 2 END AS ABN_TYP  --异常类型：0-无异常 1-异常订单 2-商家退款
        |FROM TO_YCAK_CNSM_D
        |WHERE DATA_DT = ${analyticDate}
      """.stripMargin).createTempView("TEMP_RESULT")

    //将以上结果写入到 EDS 层 TW_CNSM_BRIEF_D 消费订单流水日增量表
    sparkSession.sql(
      s"""
        | insert overwrite table TW_CNSM_BRIEF_D partition (data_dt=${analyticDate}) select * from temp_result
      """.stripMargin)
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from temp_result
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"TW_CNSM_BRIEF_D",properties)

    println("****** All Finished ******")

  }
}
