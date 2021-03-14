package cn.yasin.scala.musicproject.eds.content

import cn.yasin.scala.musicproject.common.{ConfigUtils, DateUtils}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.mutable.ListBuffer

object GenerateTwSongBaseinfoD {
    val localRun: Boolean = ConfigUtils.LOCAL_RUN
    val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
    val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
    var sparkSession:SparkSession = _

    val getAlbumName : String => String = (albumInfo:String) => {
        var albumName = ""
        try {
            val jsonArray = JSON.parseArray(albumInfo)
            albumName = jsonArray.getJSONObject(0).getString("name")
        } catch {
            case exception: Exception => {
                if (albumInfo.contains("《") && albumInfo.contains("》")) {
                    albumName = albumInfo.substring(albumInfo.indexOf('《'),albumInfo.indexOf('》') + 1)
                } else {
                    albumName = "暂无专辑"
                }
            }
        }
        albumName
    }

    val getPostTime : String => String = (postTime:String) => {
        DateUtils.formatDate(postTime)
    }

    val getSingerInfo : (String,String,String) => String = (singerInfos:String,singer:String,nameOrId:String) => {
        var singerNameOrSingerID = ""
        try {
            val jsonArray = JSON.parseArray(singerInfos)
            if ("singer1".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 0) {
                singerNameOrSingerID = jsonArray.getJSONObject(0).getString("name")
            } else if ("singer1".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 0) {
                singerNameOrSingerID = jsonArray.getJSONObject(0).getString("id")
            } else if ("singer2".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 1) {
                singerNameOrSingerID = jsonArray.getJSONObject(1).getString("name")
            } else if ("singer2".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 1) {
                singerNameOrSingerID = jsonArray.getJSONObject(1).getString("id")
            }
        } catch {
            case exception: Exception => {
                singerNameOrSingerID
            }
        }
        singerNameOrSingerID
    }

    val getAuthCompany : String => String = (authCompanyInfo:String) => {
        var authCompanyName = "乐心曲库"
        try {
            val jsonObject = JSON.parseObject(authCompanyInfo)
            authCompanyName = jsonObject.getString("name")
        } catch {
            case exception: Exception => {
                authCompanyName
            }
        }
        authCompanyName
    }

    val getPrdctType : (String => ListBuffer[Int]) = (productTypeInfo:String) => {
        val list = new ListBuffer[Int]()
        if (!"".equals(productTypeInfo.trim)) {
            val result = productTypeInfo.stripPrefix("[").stripSuffix("]").split(",")
            result.foreach( line => {
                list.append(line.toDouble.toInt)
            })
        }
        list
    }

    def main(args: Array[String]): Unit = {
        if (localRun == true) {
            sparkSession = SparkSession.builder().appName("GenerateTwSongBaseinfoD")
                .master("local").config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
        } else {
            SparkSession.builder().appName("GenerateTwSongBaseinfoD").config("hive.metastore.uris",hiveMetastoreUris)
                .enableHiveSupport().getOrCreate()
        }
        //导入隐式函数并创建UDF函数
        import org.apache.spark.sql.functions._
        val udfGetAlbumName = udf(getAlbumName)
        val udfGetPostTime = udf(getPostTime)
        val udfGetSingerInfo = udf(getSingerInfo)
        val udfGetAuthCompany = udf(getAuthCompany)
        val udfGetPrdctType = udf(getPrdctType)

        sparkSession.sql(s"use ${hiveDatabase}")
        //根据需求进行相关列的清洗
        sparkSession.table("TO_SONG_INFO_D")
            .withColumn("ALBUM",udfGetAlbumName(col("ALBUM")))
            .withColumn("POST_TIME",udfGetPostTime(col("POST_TIME")))
            .withColumn("SINGER1",udfGetSingerInfo(col("SINGER_INFO"),lit("singer1"),lit("name")))
            .withColumn("SINGER1ID",udfGetSingerInfo(col("SINGER_INFO"),lit("singer1"),lit("id")))
            .withColumn("SINGER2",udfGetSingerInfo(col("SINGER_INFO"),lit("singer2"),lit("name")))
            .withColumn("SINGER2ID",udfGetSingerInfo(col("SINGER_INFO"),lit("singer2"),lit("id")))
            .withColumn("AUTH_CO",udfGetAuthCompany(col("AUTH_CO")))
            .withColumn("PRDCT_TYPE",udfGetPrdctType(col("PRDCT_TYPE")))
            .createTempView("TEMP_TO_SONG_INFO_D")
        sparkSession.sql(
            """
              |select NBR,
              |       nvl(NAME,OTHER_NAME) as NAME,
              |       SOURCE,
              |       ALBUM,
              |       PRDCT,
              |       LANG,
              |       VIDEO_FORMAT,
              |       DUR,
              |       SINGER1,
              |       SINGER2,
              |       SINGER1ID,
              |       SINGER2ID,
              |       0 as MAC_TIME,
              |       POST_TIME,
              |       PINYIN_FST,
              |       PINYIN,
              |       SING_TYPE,
              |       ORI_SINGER,
              |       LYRICIST,
              |       COMPOSER,
              |       BPM_VAL,
              |       STAR_LEVEL,
              |       VIDEO_QLTY,
              |       VIDEO_MK,
              |       VIDEO_FTUR,
              |       LYRIC_FTUR,
              |       IMG_QLTY,
              |       SUBTITLES_TYPE,
              |       AUDIO_FMT,
              |       ORI_SOUND_QLTY,
              |       ORI_TRK,
              |       ORI_TRK_VOL,
              |       ACC_VER,
              |       ACC_QLTY,
              |       ACC_TRK_VOL,
              |       ACC_TRK,
              |       WIDTH,
              |       HEIGHT,
              |       VIDEO_RSVL,
              |       SONG_VER,
              |       AUTH_CO,
              |       STATE,
              |       case when size(PRDCT_TYPE) =0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
              |    from TEMP_TO_SONG_INFO_D
              |    where NBR != ''
              |""".stripMargin).write.format("Hive").mode(saveMode = "overwrite").saveAsTable("TW_SONG_BASEINFO_D")
        println("****** All Finished ******")
    }
}