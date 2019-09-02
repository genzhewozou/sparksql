package Log.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换(输入==>输出)工具类
  */
object AccessConvertUtil {

  //定义输出字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出样式
    *
    * @param log
    */
  def parseLog(log: String) = {
    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)

      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = "" //IpUtil.getCity(ip)

      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //这个row里面的字段要和struct结构体里面的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
