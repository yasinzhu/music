package cn.yasin.scala.musicproject.eds.user

import cn.yasin.scala.musicproject.common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTwUsrBaseinfoD {
    private val localRun: Boolean = ConfigUtils.LOCAL_RUN
    private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
    private val hiveDataBase = ConfigUtils.HIVE_DATABASE
    private var sparkSession: SparkSession = _

    private val mysqlUrl = ConfigUtils.MYSQL_URL
    private val mysqlUser = ConfigUtils.MYSQL_USER
    private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println(s"需要指定数据日期,格式为(YYYYMMDD)")
            System.exit(1)
        }
        if (localRun == true) {
            sparkSession = SparkSession.builder().master("local").appName("Generate_TW_Song_Rsi_D")
                .config("spark.sql.shuffle.partitions", "1")
                .config("hive.metastore.uris", hiveMetaStoreUris).enableHiveSupport().getOrCreate()
            sparkSession.sparkContext.setLogLevel("Error")
        } else {
            sparkSession = SparkSession.builder().appName("Generate_TW_Song_Rsi_D").enableHiveSupport().getOrCreate()
        }

        val currentDate = args(0)
        sparkSession.sql(s"use ${hiveDataBase}")
        val usrWx = sparkSession.sql(
            """
              | SELECT
              |  UID,       --用户ID
              |  REG_MID,   --机器ID
              |  "1" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
              |  WX_ID AS REF_UID,  --微信账号
              |  GDR,               --性别
              |  BIRTHDAY,          --生日
              |  MSISDN,            --手机号码
              |  LOC_ID,            --地区ID
              |  LOG_MDE,           --注册登录方式
              |  substring(REG_TM,1,8) AS REG_DT,   --注册日期
              |  substring(REG_TM,9,6) AS REG_TM,   --注册时间
              |  USR_EXP,           --用户当前经验值
              |  SCORE,             --累计积分
              |  LEVEL,             --用户等级
              |  "2" AS USR_TYPE,   --用户类型 1-企业 2-个人
              |  NULL AS IS_CERT,   --实名认证
              |  NULL AS IS_STDNT   --是否是学生
              |FROM TO_YCAK_USR_D
      """.stripMargin)
        var usrAli = sparkSession.sql(
            """
              | SELECT
              |  UID,       --用户ID
              |  REG_MID,   --机器ID
              |  "2" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
              |  ALY_ID AS REF_UID,  --支付宝账号
              |  GDR,               --性别
              |  BIRTHDAY,          --生日
              |  MSISDN,            --手机号码
              |  LOC_ID,            --地区ID
              |  LOG_MDE,           --注册登录方式
              |  substring(REG_TM,1,8) AS REG_DT,   --注册日期
              |  substring(REG_TM,9,6) AS REG_TM,   --注册时间
              |  USR_EXP,           --用户当前经验值
              |  SCORE,             --累计积分
              |  LEVEL,             --用户等级
              |  NVL(USR_TYPE,"2") AS USR_TYPE,   --用户类型 1-企业 2-个人
              |  IS_CERT ,                  --实名认证
              |  IS_STDNT                   --是否是学生
              |FROM TO_YCAK_USR_ALI_D
      """.stripMargin)
        val usrQQ = sparkSession.sql(
            """
              |SELECT
              | UID,       --用户ID
              | REG_MID,   --机器ID
              | "3" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
              | QQID AS REF_UID,  --QQ账号
              | GDR,               --性别
              | BIRTHDAY,          --生日
              | MSISDN,            --手机号码
              | LOC_ID,            --地区ID
              | LOG_MDE,           --注册登录方式
              | substring(REG_TM,1,8) AS REG_DT,   --注册日期
              | substring(REG_TM,9,6) AS REG_TM,   --注册时间
              | USR_EXP,           --用户当前经验值
              | SCORE,             --累计积分
              | LEVEL,             --用户等级
              | "2" AS USR_TYPE,   --用户类型 1-企业 2-个人
              | NULL AS IS_CERT,   --实名认证
              | NULL AS IS_STDNT   --是否是学生
              |FROM TO_YCAK_USR_QQ_D
      """.stripMargin)
        val usrApp = sparkSession.sql(
            """
              |SELECT
              | UID,       --用户ID
              | REG_MID,   --机器ID
              | "4" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
              | APP_ID AS REF_UID,  --APP账号
              | GDR,               --性别
              | BIRTHDAY,          --生日
              | MSISDN,            --手机号码
              | LOC_ID,            --地区ID
              | NULL AS LOG_MDE,           --注册登录方式
              | substring(REG_TM,1,8) AS REG_DT,   --注册日期
              | substring(REG_TM,9,6) AS REG_TM,   --注册时间
              | USR_EXP,           --用户当前经验值
              | 0 AS SCORE,        --累计积分
              | LEVEL,             --用户等级
              | "2" AS USR_TYPE,   --用户类型 1-企业 2-个人
              | NULL AS IS_CERT,   --实名认证
              | NULL AS IS_STDNT   --是否是学生
              |FROM TO_YCAK_USR_APP_D
      """.stripMargin)
        val allusrInfo = usrWx.union(usrAli).union(usrQQ).union(usrApp)
        sparkSession.table("TO_YCAK_USR_LOGIN_D")
            .where(s"data_dt = $currentDate")
            .select("UID")
            .distinct()
            .join(allusrInfo, Seq("UID"), "left")
            .createTempView("TEMP_USR_ACTV")
        sparkSession.sql(
            s"""
               | insert overwrite table TW_USR_BASEINFO_D partition (data_dt = ${currentDate})
               | select * from TEMP_USR_ACTV
      """.stripMargin)
        val pre7Date = DateUtils.getCurrentDatePreDate(currentDate, 7)
        val properties = new Properties()
        properties.setProperty("user", mysqlUser)
        properties.setProperty("password", mysqlPassword)
        properties.setProperty("driver", "com.mysql.jdbc.Driver")
        sparkSession.sql(
            s"""
               | select
               |     A.UID, --用户ID
               |     CASE WHEN B.REG_CHNL = '1' THEN '微信'
               |          WHEN B.REG_CHNL = '2' THEN '支付宝'
               |          WHEN B.REG_CHNL = '3' THEN 'QQ'
               |          WHEN B.REG_CHNL = '4' THEN 'APP'
               |          ELSE '未知' END REG_CHNL,   --注册渠道
               |     B.REF_UID,    --账号ID
               |     CASE WHEN B.GDR = '0' THEN '不明'
               |          WHEN B.GDR = '1' THEN '男'
               |          WHEN B.GDR = '2' THEN '女'
               |          ELSE '不明' END GDR,        --性别
               |     B.BIRTHDAY,   --生日
               |     B.MSISDN,     --手机号码
               |     B.REG_DT,     --注册日期
               |     B.LEVEL       --用户等级
               | from
               |  (
               |     select
               |       UID,count(*) as c
               |     from TW_USR_BASEINFO_D
               |     where data_dt between ${pre7Date} and ${currentDate}
               |     group by UID having c = 1   -- 注意：这里应该写7 ，因为计算的是7日用户活跃情况。
               |  ) A,
               | TW_USR_BASEINFO_D B
               | where B.data_dt =  ${currentDate} and A.UID = B.UID
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "user_7days_active", properties)
        println("****** All Finished ******")
    }


}
