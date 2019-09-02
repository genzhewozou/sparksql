package DataFrame_Set

import org.apache.spark.sql.SparkSession

object DataFrameRDD {
  /*
  DataFrame与RDD的交互操作,使用临时表操作
   */
  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/test/info.txt"

    val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

    //RDD ==> DataFrame
    //1 获取RDD（String类型）
    val rdd = sparkSession.sparkContext.textFile(inputPath)

    //注意：需要导入隐式转换
    import sparkSession.implicits._
    val indoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    indoDF.show()

    //创建或替换为临时表，后续可以采用sql处理
    indoDF.createOrReplaceTempView("infos")
    sparkSession.sql("select * from infos where age>30").show()

    sparkSession.stop()
  }

  case class Info(id: Int, name: String, age: Int)  //采用反射方式

}
