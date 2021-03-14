package cn.yasin.scala.musicproject.ods

import cn.yasin.scala.musicproject.base.PairRDDMultipleTextOutPutFormat
import cn.yasin.scala.musicproject.common.ConfigUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProductClientLog {
    private val localRun: Boolean = ConfigUtils.LOCAL_RUN
    private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
    private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
    private val clientLogHDFSPath: String = ConfigUtils.CLIENTLOG_HDFS_PATH
    private var sparkSession: SparkSession = _
    private var sc: SparkContext = _
    private var clientLogInfos: RDD[String] = _

    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println(s"需要指定数据日期,格式为(YYYYMMDD)")
            System.exit(1)
        }
        //获取指定的数据日期
        val logDate = args(0)
        //判断运行的方式
        if (localRun == true) {
            sparkSession = SparkSession.builder().appName("ProductClientLog").master("local")
                .config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
            sc = sparkSession.sparkContext
            clientLogInfos = sc.textFile(s"${clientLogHDFSPath}/currentday_clientlog.tar.gz")
        } else {
            sparkSession = SparkSession.builder().appName("ProductClientLog")
                .config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
            sc = sparkSession.sparkContext
            clientLogInfos = sc.textFile(s"${clientLogHDFSPath}/currentday_clientlog.tar.gz")
        }
        //根据&进行切割,然后拿到表名和json串
        val tableNameAndInfos = clientLogInfos.map(line => line.split("&"))
            .filter(item => item.size == 6)
            .map(line => (line(2), line(3)))
        //获取符合条件的表json串信息,其余的直接保存json串,统一保存到hdfs中
        tableNameAndInfos.map(tp => {
            val tableName = tp._1
            val tableInfos = tp._2
            if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(tableName)) {
                val jsonObject = JSON.parseObject(tableInfos)
                val songId = jsonObject.getString("songid")
                val mid = jsonObject.getString("mid")
                val optrateType = jsonObject.getString("optrate_type")
                val uid = jsonObject.getString("uid")
                val consumeType = jsonObject.getString("consume_type")
                val durTime = jsonObject.getString("dur_time")
                val sessionId = jsonObject.getString("session_id")
                val songName = jsonObject.getString("songname")
                val pkgId = jsonObject.getString("pkg_id")
                val orderId = jsonObject.getString("order_id")
                (tableName, songId + "\t" + mid + "\t" + optrateType + "\t" + uid + "\t" + consumeType + "\t" + durTime
                    + "\t" + sessionId + "\t" + songName + "\t" + pkgId + "\t" + orderId)
            } else {
                tp
            }
        }).saveAsHadoopFile(
            s"${clientLogHDFSPath}/all_client_tables/${logDate}",
            classOf[String],
            classOf[String],
            classOf[PairRDDMultipleTextOutPutFormat]
        )
        //执行hive sql
        sparkSession.sql(s"use ${hiveDatabase}")
        sparkSession.sql(
            """
              |CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
              | `SONGID` string,
              | `MID` string,
              | `OPTRATE_TYPE` string,
              | `UID` string,
              | `CONSUME_TYPE` string,
              | `DUR_TIME` string,
              | `SESSION_ID` string,
              | `SONGNAME` string,
              | `PKG_ID` string,
              | `ORDER_ID` string
              |)
              |partitioned by (data_dt string)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
              |LOCATION 'hdfs://sxt01:9000/user/hive/warehouse/data/song/'
              |""".stripMargin
        )
        sparkSession.sql(
            s"""
               | load data inpath
               | '${clientLogHDFSPath}/all_client_tables/${logDate}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
               | into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt='${logDate}')
               |""".stripMargin
        )
        println("****** All Finished ******")
    }
}
