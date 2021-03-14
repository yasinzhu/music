package cn.yasin.scala.musicproject.eds.content

import cn.yasin.scala.musicproject.common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.SparkSession

object GenerateTwSongFturD {
    private val localRun: Boolean = ConfigUtils.LOCAL_RUN
    private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
    private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
    var sparkSession : SparkSession = _

    def main(args: Array[String]): Unit = {
        if (args.length < 0) {
            println(s"需要指定数据日期,格式为(YYYYMMDD)")
            System.exit(1)
        }
        if (localRun == true) {
            sparkSession = SparkSession.builder().appName("GenerateTwSongFturD").master("local").config("hive.metastore.uris",hiveMetastoreUris)
                .config("spark.sql.shuffle.partitions","1").enableHiveSupport().getOrCreate()
//            sparkSession.sparkContext.setLogLevel("Error")
        } else {
            sparkSession = SparkSession.builder().appName("GenerateTwSongFturD").config("hive.metastore.uris",hiveMetastoreUris).enableHiveSupport().getOrCreate()
        }
        val analyticDate = args(0)
        val per7Date = DateUtils.getCurrentDatePreDate(analyticDate, 7)
        val per30Date = DateUtils.getCurrentDatePreDate(analyticDate, 30)
        println(analyticDate + ":" + per7Date + ":" + per30Date)
        sparkSession.sql(s"use ${hiveDatabase}")
        sparkSession.sql(
            s"""
              | select
              | 	 songid as NBR,   --歌曲ID
              | 	 count(*) as SING_CNT,  --当日点唱量
              | 	 0 as SUPP_CNT ,        --当日点赞量
              | 	 count(distinct uid) as USR_CNT,  --当日点唱用户数
              | 	 count(distinct order_id) as ORDR_CNT --当日点唱订单数
              | from TO_CLIENT_SONG_PLAY_OPERATE_REQ_D
              | where data_dt = ${analyticDate}
              | group by songid
              |""".stripMargin).createTempView("currentDayTable")
        sparkSession.sql(
            s"""
               | select
               | 	 songid as NBR,     --歌曲ID
               | 	 count(*) as RCT_7_SING_CNT,    --近七天点唱量
               | 	 0 as RCT_7_SUPP_CNT ,          --近七天点赞量
               | 	 count(distinct uid) as RCT_7_USR_CNT,  --近七天点唱用户数
               | 	 count(distinct order_id) as RCT_7_ORDR_CNT --近七天点唱订单数
               | from to_client_song_play_operate_req_d
               | where  ${per7Date}<= data_dt and data_dt <= ${analyticDate}
               | group by songid
               |""".stripMargin).createTempView("pre7DayTable")
        sparkSession.sql(
            s"""
               | select
               | 	 songid as NBR,         --歌曲ID
               | 	 count(*) as RCT_30_SING_CNT,   --近三十天点唱量
               | 	 0 as RCT_30_SUPP_CNT ,         --近三十天点赞量
               | 	 count(distinct uid) as RCT_30_USR_CNT,   --近三十天点唱用户数
               | 	 count(distinct order_id) as RCT_30_ORDR_CNT  --近三十天点唱订单数
               | from to_client_song_play_operate_req_d
               | where  ${per30Date}<= data_dt and data_dt <= ${analyticDate}
               | group by songid
               |""".stripMargin).createTempView("pre30DayTable")
        sparkSession.sql(
            s"""
               | select
               |  NBR,    --歌曲ID
               |  max(case when DATA_DT BETWEEN ${per7Date} and ${analyticDate} then SING_CNT else 0 end) as RCT_7_TOP_SING_CNT,    --近七天最高日点唱量
               |  max(case when DATA_DT BETWEEN ${per7Date} and ${analyticDate} then SUPP_CNT else 0 end) as RCT_7_TOP_SUPP_CNT,    --近七天最高日点赞量
               |  max(SING_CNT) as RCT_30_TOP_SING_CNT,   --近三十天最高日点唱量
               |  max(SUPP_CNT) as RCT_30_TOP_SUPP_CNT    --近三十天最高日点赞量
               | from TW_SONG_FTUR_D
               | where DATA_DT BETWEEN ${per30Date} and  ${analyticDate}
               | group by NBR
               |""".stripMargin).createTempView("pre7And30DayInfoTable")
        sparkSession.sql(
            s"""
               |select
               | A.NBR,        --歌曲编号
               | B.NAME,       --歌曲名称
               | B.SOURCE,     --来源
               | B.ALBUM,      --所属专辑
               | B.PRDCT,      --发行公司
               | B.LANG,       --语言
               | B.VIDEO_FORMAT, --视频风格
               | B.DUR,          --时长
               | B.SINGER1,      --歌手1
               | B.SINGER2,      --歌手2
               | B.SINGER1ID,    --歌手1ID
               | B.SINGER2ID,    --歌手2ID
               | B.MAC_TIME,     --加入机器时间
               | A.SING_CNT,     --当日点唱量
               | A.SUPP_CNT,     --当日点赞量
               | A.USR_CNT,      --当日点唱用户数
               | A.ORDR_CNT,     --当日点唱订单数
               | nvl(C.RCT_7_SING_CNT,0) as RCT_7_SING_CNT,  --近7天点唱量
               | nvl(C.RCT_7_SUPP_CNT,0) as RCT_7_SUPP_CNT,   --近7天点赞量
               | nvl(E.RCT_7_TOP_SING_CNT,0) as RCT_7_TOP_SING_CNT,  --近7天最高点唱量
               | nvl(E.RCT_7_TOP_SUPP_CNT,0) as RCT_7_TOP_SUPP_CNT,  --近7天最高点赞量
               | nvl(C.RCT_7_USR_CNT,0) as RCT_7_USR_CNT,       --近7天点唱用户数
               | nvl(C.RCT_7_ORDR_CNT,0) as RCT_7_ORDR_CNT,     --近7天点唱订单数
               | nvl(D.RCT_30_SING_CNT,0) as RCT_30_SING_CNT,   --近30天点唱量
               | nvl(D.RCT_30_SUPP_CNT,0) as RCT_30_SUPP_CNT,   --近30天点赞量
               | nvl(E.RCT_30_TOP_SING_CNT,0) as RCT_30_TOP_SING_CNT,  --近30天最高点唱量
               | nvl(E.RCT_30_TOP_SUPP_CNT,0) as RCT_30_TOP_SUPP_CNT,   --近30天最高点赞量
               | nvl(D.RCT_30_USR_CNT,0) as RCT_30_USR_CNT,      --近30天点唱用户数
               | nvl(D.RCT_30_ORDR_CNT,0) as RCT_30_ORDR_CNT     --近30天点唱订单数
               |from
               | currentDayTable A
               | JOIN TW_SONG_BASEINFO_D B ON A.NBR = B.NBR
               | LEFT JOIN pre7DayTable C ON A.NBR = C.NBR
               | LEFT JOIN pre30DayTable D ON A.NBR = D.NBR
               | LEFT JOIN pre7And30DayInfoTable E ON A.NBR = E.NBR
               |""".stripMargin).createTempView("result")
        sparkSession.sql(
            s"""
               |insert overwrite table tw_song_ftur_d partition(data_dt=${analyticDate}) select * from result
               |""".stripMargin)
        println("****** All Finished ******")
    }
}