package cn.yasin.scala.musicproject.base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class PairRDDMultipleTextOutPutFormat extends MultipleTextOutputFormat[Any,Any] {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
        val fileName = key.asInstanceOf[String]
        fileName
    }

    override def generateActualKey(key: Any, value: Any): String = {
        null
    }
}