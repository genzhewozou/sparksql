package ExternalDataSource

import org.apache.spark.sql.SparkSession

/*
parquet文件操作
 */
object ParquetAPP {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("parquet").getOrCreate()

    val inputPath = "hdfs://192.168.29.17:9000/test/users.parquet"

    val userDF = sparkSession.read.format("parquet").load(inputPath)

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show()  //读取指定列
    //写数据 写到了
    userDF.select("name","favorite_color").write.format("json").save("/root/jsonOut")

    //默认加载parquet类型
    sparkSession.read.load(inputPath)

    sparkSession.stop()
  }
}
