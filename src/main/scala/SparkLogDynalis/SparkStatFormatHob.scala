package SparkLogDynalis

import SparkLogDynalis.Util.DataUtils
import org.apache.spark.sql.SparkSession

/*
第一步清洗，抽取出我们需要的指定列的数据
 */
object SparkStatFormatHob {

  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/sparkLogProgect/10000_access.log"

    val spark = SparkSession.builder().master("local")
      .appName("SparkStatFormatHob").getOrCreate()

    val log = spark.sparkContext.textFile(inputPath) //获取日志数据

    //    log.take(10).foreach(println)
    log.map(line => {
      val splits = line.split(" ") //按空格进行分割
      val ip = splits(0)

      //原始日志数据的第三个字段和第四个字段拼接起来就是访问时间 [10/Nov/2016:00:01:02 +0800]
      val time = splits(3) + " " + splits(4)

      val url = splits(11)

      val traffic = splits(9) //流量

      //      (DataUtils.prase(time), url, traffic, ip) //组成元组测试使用
      DataUtils.prase(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("hdfs://192.168.29.17:9000/sparkLogProgect/firstLog")  //写到hdfs里面

    spark.stop()
  }
}
