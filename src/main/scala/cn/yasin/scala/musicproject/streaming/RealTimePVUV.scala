package cn.yasin.scala.musicproject.streaming

import cn.yasin.scala.musicproject.base.RedisClient
import cn.yasin.scala.musicproject.common.ConfigUtils
import com.alibaba.fastjson.JSON
import org.apache.commons.lang.mutable.Mutable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}

import java.sql.{Connection, DriverManager, Statement}
import java.{lang, util}
import scala.collection.mutable

case class pvInfo(mid:String,pv:String)
case class uvInfo(mid:String,uv:String)

object RealTimePVUV {
    private val localRun: Boolean = ConfigUtils.LOCAL_RUN
    private val redisDB: Int = ConfigUtils.REDIS_DB
    private val redisOffsetDB: Int = ConfigUtils.REDIS_OFFSET_DB
    private val kafkaCluster: String = ConfigUtils.KAFKA_CLUSTER
    private val topic: String = ConfigUtils.KAFKA_USERPLAYSONG_TOPIC
    private val mysqlUrl: String = ConfigUtils.MYSQL_URL
    private val mysqlUser: String = ConfigUtils.MYSQL_USER
    private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD
    var sparkSession: SparkSession = _
    var sc: SparkContext = _

    def getOffSetFromRedis(db: Int, tp: String): mutable.Map[String, String] = {
        val jRedis = RedisClient.pool.getResource
        jRedis.select(db)
        val result: util.Map[String, String] = jRedis.hgetAll(topic)
        RedisClient.pool.returnResource(jRedis)
        if (result.size() == 0) {
            result.put("0", "0")
            result.put("1", "0")
            result.put("2", "0")
        }
        import scala.collection.JavaConversions.mapAsScalaMap
        val offsetMap: scala.collection.mutable.Map[String, String] = result
        offsetMap
    }

    def savePVToRedis(redisDB: Int, iter: Iterator[(String, Int)]): Unit = {
        val jedis = RedisClient.pool.getResource
        jedis.select(redisDB)
        val pipeline = jedis.pipelined()
        iter.foreach(tp => {
            pipeline.hset("pv", tp._1, tp._2.toString)
        })
        pipeline.sync()
        RedisClient.pool.returnResource(jedis)
    }

    def saveUVToRedis(redisDB: Int, iter: Iterator[(String, Int)]): Unit = {
        val jedis = RedisClient.pool.getResource
        jedis.select(redisDB)
        val pipeline = jedis.pipelined()
        iter.foreach(tp => {
            pipeline.hset("uv", tp._1, tp._2.toString)
        })
        pipeline.sync()
        RedisClient.pool.returnResource(jedis)
    }

    def saveOffsetToRedis(db: Int, offsetRanges: Array[OffsetRange]) = {
        val jedis = RedisClient.pool.getResource
        jedis.select(db)
        offsetRanges.foreach(offsetRange => {
            println(s"topic:${offsetRange.topic}  partition:${offsetRange.partition}  fromOffset:${offsetRange.fromOffset}  untilOffset: ${offsetRange.untilOffset}")
            jedis.hset(offsetRange.topic, offsetRange.partition.toString, offsetRange.untilOffset.toString)
        })
        RedisClient.pool.returnResource(jedis)
    }

    def main(args: Array[String]): Unit = {
        if (localRun) {
            sparkSession = SparkSession.builder().appName("RealTimePVUV").master("local")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
            sc = sparkSession.sparkContext
        } else {
            sparkSession = SparkSession.builder().appName("RealTimePVUV")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
            sc = sparkSession.sparkContext
        }
        sparkSession.sparkContext.setLogLevel("Error")
        val ssc = new StreamingContext(sc, Durations.seconds(5))
        val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(redisOffsetDB, topic)
        val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map {
            resultSet => new TopicPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
        }.toMap
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> kafkaCluster,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "MyGroup",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: lang.Boolean)
        )
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            ssc, PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
        stream.map(cr => {
            val jsonObject = JSON.parseObject(cr.value())
            val mid = jsonObject.getString("mid")
            val uid = jsonObject.getString("uid")
            (mid, 1)
        }).reduceByKeyAndWindow(
            (v1: Int, v2: Int) => {
                v1 + v2
            },
            Durations.minutes(1),
            Durations.seconds(5)
        ).foreachRDD(
            rdd => {
//                rdd.foreachPartition(iter => {
//                    savePVToRedis(redisDB, iter)
//                })
                val pv: RDD[pvInfo] = rdd.map(tp => {
                    val mid = tp._1
                    val pv = tp._2.toString
                    pvInfo(mid,pv)
                })
                val session = sparkSession.newSession()
                import session.implicits._
                pv.toDF().createTempView("temp_tw_pv")
                session.sql(
                    s"""
                       |select mid,pv from temp_tw_pv
                       |""".stripMargin)
                    .write.format("jdbc").mode(SaveMode.Overwrite)
                    .option("url",mysqlUrl)
                    .option("user",mysqlUser)
                    .option("password",mysqlPassword)
                    .option("driver","com.mysql.jdbc.Driver")
                    .option("dbtable","tw_pv")
                    .save()
            }
        )

        stream.window(Durations.seconds(60), Durations.seconds(5))
            .map(cr => {
                val jsonObject = JSON.parseObject(cr.value())
                val mid = jsonObject.getString("mid")
                val uid = jsonObject.getString("uid")
                (mid, uid)
            }).transform(rdd => {
            val distinctRDD = rdd.distinct()
            distinctRDD.map(tp => {
                (tp._1, 1)
            }).reduceByKey((v1: Int, v2: Int) => {
                v1 + v2
            })
        }).foreachRDD(rdd => {
//            rdd.foreachPartition(iter => {
//                saveUVToRedis(redisDB, iter)
//            })
            val uv = rdd.map(tp => {
            val mid = tp._1
            val uv = tp._2.toString
            uvInfo(mid, uv)
            })
            val session = sparkSession.newSession()
            import session.implicits._
            uv.toDF().createTempView("temp_tw_uv")
            session.sql(
                s"""
                   |select mid,uv from temp_tw_uv
                   |""".stripMargin)
                .write.format("jdbc").mode(SaveMode.Overwrite)
                .option("url",mysqlUrl)
                .option("user",mysqlUser)
                .option("password",mysqlPassword)
                .option("driver","com.mysql.jdbc.Driver")
                .option("dbtable","tw_uv")
                .save()
            }
        )
        stream.foreachRDD { (rdd: RDD[ConsumerRecord[String, String]]) =>
            print("****** All Finished ******")
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            saveOffsetToRedis(redisOffsetDB, offsetRanges)
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
