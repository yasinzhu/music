package cn.yasin.scala.musicproject.ods

import cn.yasin.scala.musicproject.base.RedisClient
import cn.yasin.scala.musicproject.common.ConfigUtils
import cn.yasin.scala.musicproject.streaming.RealTimePVUV.{mysqlPassword, mysqlUrl, mysqlUser}
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager, ResultSet}
import java.util

object Test {
    def main(args: Array[String]): Unit = {
//        val redisdb = ConfigUtils.REDIS_DB
//        val sparkSession = SparkSession.builder().appName("Test").master("local[2]")
//            .config("hive.metastore.uris", "thrift://sxt02:9083").enableHiveSupport().getOrCreate()
//        val sparkContext = sparkSession.sparkContext
//        val dataFrame = sparkSession.sql("show databases")
//        dataFrame.foreach(line => println(line))
//        val lines = sparkContext.textFile("dataset/test.txt")
//        val words = lines.flatMap(line => line.split(" "))
//        val map = words.map(line => (line, 1))
//        val result = map.reduceByKey((word, count) => (word + count))
//        result.foreach(line => println(line._1 + ":" + line._2))
//        val jedis = RedisClient.pool.getResource
//        var cnt = 0
//        jedis.select(redisdb)
//        val str: util.Map[String, String] = jedis.hgetAll("uv")
//        println(str.get("97724"))
//        val value: util.Iterator[String] = str.keySet().iterator()
//        while (value.hasNext) {
//            val result: String = value.next()
//            cnt = cnt + str.get(result).toInt
//        }
//        println(cnt)
        val mysqlurl = ConfigUtils.MYSQL_URL
        val mysqluser = ConfigUtils.MYSQL_USER
        val mysqlpassword = ConfigUtils.MYSQL_PASSWORD
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        val connection = DriverManager.getConnection(mysqlurl,mysqluser,mysqlpassword)
        val statement = connection.createStatement()
        val sql = s"select * from tw_pv where mid = '${91236}'"
        val set: ResultSet = statement.executeQuery(sql)
        println(set.next())


    }
}
