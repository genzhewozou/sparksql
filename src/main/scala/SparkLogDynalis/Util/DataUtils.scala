package SparkLogDynalis.Util

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

object DataUtils {

  //[10/Nov/2016:00:01:02 +0800]  使用FastDataFormat
  val YYYYMMDDHHHH_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标日期格式
  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def prase(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间 long类型
    *
    * @param time
    * @return
    */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHHH_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    print(prase("[10/Nov/2016:00:01:02 +0800]"))
  }
}
