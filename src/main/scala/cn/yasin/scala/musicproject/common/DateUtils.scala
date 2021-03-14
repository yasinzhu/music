package cn.yasin.scala.musicproject.common

import java.text.SimpleDateFormat
import java.math.BigDecimal
import java.util.{Calendar, Date}

object DateUtils {
    //解析数据里面的日期
    def formatDate(stringDate:String): String = {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var formatDate = ""
        try {
            formatDate = sdf.format(sdf.parse(stringDate))
        } catch {
            case exception: Exception => {
                try {
                    val bigDecimal = new BigDecimal(stringDate)
                    val date = new Date(bigDecimal.longValue())
                    formatDate = sdf.format(date)
                } catch {
                    case exception: Exception => {
                        formatDate
                    }
                }
            }
        }
        formatDate
    }
    //根据当前日期获取指定天数的日期
    def getCurrentDatePreDate(currentDate:String, i:Int) = {
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val date = sdf.parse(currentDate)
        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.add(Calendar.DATE,-i)
        val per7Date = calendar.getTime
        sdf.format(per7Date)
    }
}
