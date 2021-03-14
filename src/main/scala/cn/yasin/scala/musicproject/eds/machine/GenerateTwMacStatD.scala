package cn.yasin.scala.musicproject.eds.machine

import cn.yasin.scala.musicproject.common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTwMacStatD {

  val localRun : Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBase = ConfigUtils.HIVE_DATABASE
  var sparkSession : SparkSession = _

  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {

    if(localRun){
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris",hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions",10)
        .enableHiveSupport().getOrCreate()
    }else{
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions",10).enableHiveSupport().getOrCreate()
    }

    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }
    val analyticDate = args(0)

    sparkSession.sparkContext.setLogLevel("Error")
    sparkSession.sql(s"use $hiveDataBase ")

    sparkSession.table("TW_MAC_BASEINFO_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_MAC_BASEINFO_D")

    sparkSession.table("TW_MAC_LOC_D").where(s"data_dt = ${analyticDate}")
        .createTempView("TW_MAC_LOC_D")

    sparkSession.table("TW_CNSM_BRIEF_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_CNSM_BRIEF_D")

    sparkSession.table("TW_USR_BASEINFO_D").where(s"data_dt = ${analyticDate}")
      .createTempView("TW_USR_BASEINFO_D")

    sparkSession.sql(
      """
        | select
        |   MID,            --机器ID
        |   PKG_ID,         --套餐ID
        |   PAY_TYPE,       --支付类型
        |   COUNT(DISTINCT UID) as CNSM_USR_CNT, --总消费用户数
        |   SUM(COIN_CNT * COIN_PRC) as TOT_REV, --总营收
        |   COUNT(ORDR_ID) as REV_ORDR_CNT  --总营收订单数
        | from TW_CNSM_BRIEF_D
        | where ABN_TYP = 0
        | group by MID,PKG_ID,PAY_TYPE
      """.stripMargin).createTempView("TEMP_REV")

    sparkSession.sql(
      """
        | select
        |   MID,            --机器ID
        |   PKG_ID,         --套餐ID
        |   PAY_TYPE,       --支付类型
        |   COUNT(DISTINCT UID) as REF_USR_CNT, --总退款用户数
        |   SUM(COIN_CNT * COIN_PRC) as TOT_REF, --总退款
        |   COUNT(ORDR_ID) as REF_ORDR_CNT  --总退款订单数
        | from TW_CNSM_BRIEF_D
        | where ABN_TYP = 2
        | group by MID,PKG_ID,PAY_TYPE
      """.stripMargin).createTempView("TEMP_REF")

    sparkSession.sql(
      s"""
        |select
        | REG_MID as MID,   --机器ID
        | count(UID) as NEW_USR_CNT     --新增用户个数
        |from TW_USR_BASEINFO_D
        |where data_dt = ${analyticDate}
        |group by REG_MID
      """.stripMargin).createTempView("TEMP_USR_NEW")

    sparkSession.sql(
      """
        |SELECT
        | A.MID,          --机器ID
        | A.MAC_NM,       --机器名称
        | A.PRDCT_TYP,    --产品类型
        | A.STORE_NM,     --门店名称
        | A.BUS_MODE,     --运营模式
        | A.PAY_SW,       --是否开通移动支付
        | A.SCENCE_CATGY, --主场景分类
        | A.SUB_SCENCE_CATGY, --子场景分类
        | A.SCENE,        --主场景
        | A.SUB_SCENE,    --子场景
        | A.BRND,         --主场景品牌
        | A.SUB_BRND,     --子场景品牌
        | NVL(B.PRVC,A.PRVC) AS PRVC,       --省份
        | NVL(B.CTY,A.CTY) AS CTY,          --城市
        | NVL(B.DISTRICT,A.AREA) AS AREA,   --区县
        | A.PRTN_NM as AGE_ID,              --代理人ID
        | A.INV_RATE,     --投资人分成比例
        | A.AGE_RATE,     --代理人、联盟人分成比例
        | A.COM_RATE,     --公司分成比例
        | A.PAR_RATE,     --合作方分成比例
        | C.PKG_ID,       --套餐ID
        | C.PAY_TYPE,     --支付类型
        | NVL(C.CNSM_USR_CNT,0) AS CNSM_USR_CNT,     --总消费用户数
        | NVL(D.REF_USR_CNT,0) AS REF_USR_CNT,       --总退款用户数
        | NVL(E.NEW_USR_CNT,0) AS NEW_USR_CNT,       --总新增用户数
        | NVL(C.REV_ORDR_CNT,0) AS REV_ORDR_CNT,     --总营收订单数
        | NVL(D.REF_ORDR_CNT,0) AS REF_ORDR_CNT,     --总退款订单数
        | NVL(C.TOT_REV,0) AS TOT_REV,               --总营收
        | NVL(D.TOT_REF,0) AS TOT_REF                --总退款
        |FROM TW_MAC_BASEINFO_D A                    --机器基础信息
        |LEFT JOIN TW_MAC_LOC_D B on A.MID = B.MID   --机器当日位置信息
        |LEFT JOIN TEMP_REV C on A.MID = C.MID       --机器当日营收信息
        |LEFT JOIN TEMP_REF D on A.MID = D.MID
        |      AND C.MID = D.MID
        |      AND C.PKG_ID = D.PKG_ID
        |      AND C.PAY_TYPE = D.PAY_TYPE           --机器当日退款信息
        |LEFT JOIN TEMP_USR_NEW E on A.MID = E.MID   --机器当日新增用户信息
      """.stripMargin).createTempView("TEMP_MAC_RESULT")

    sparkSession.sql(
      s"""
        | insert overwrite table TW_MAC_STAT_D partition (data_dt = ${analyticDate}) select * from TEMP_MAC_RESULT
      """.stripMargin)

    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from TEMP_MAC_RESULT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_machine_rev_infos",properties)

    println("****** All Finished ******")
  }
}
