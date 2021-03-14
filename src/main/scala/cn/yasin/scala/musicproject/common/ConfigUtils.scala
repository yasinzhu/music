package cn.yasin.scala.musicproject.common

import com.typesafe.config.ConfigFactory

object ConfigUtils {
    lazy val load = ConfigFactory.load()
    val LOCAL_RUN: Boolean = load.getBoolean("local.run")
    val HIVE_METASTORE_URIS: String = load.getString("hive.metastore.uris")
    val HIVE_DATABASE: String = load.getString("hive.database")
    val CLIENTLOG_HDFS_PATH: String = load.getString("clientlog.hdfs.path")
    val MYSQL_URL: String = load.getString("mysql.url")
    val MYSQL_USER: String = load.getString("mysql.user")
    val MYSQL_PASSWORD: String = load.getString("mysql.password")
    val KAFKA_USERLOGININFO_TOPIC: String = load.getString("kafka.userlogininfo.topic")
    val KAFKA_USERPLAYSONG_TOPIC: String = load.getString("kafka.userplaysong.topic")
    val KAFKA_CLUSTER: String = load.getString("kafka.cluster")
    val REDIS_HOST: String = load.getString("redis.host")
    val REDIS_PORT: Int = load.getInt("redis.port")
    val REDIS_OFFSET_DB: Int = load.getInt("redis.offset.db")
    val REDIS_DB: Int = load.getInt("redis.db")
}
