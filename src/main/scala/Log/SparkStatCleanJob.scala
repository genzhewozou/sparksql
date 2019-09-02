package Log

import Log.util.AccessConvertUtil
import org.apache.spark.sql.SparkSession


/**
  * 1 使用spark进行数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/sparkLogProgect/access.log"

    val spark = SparkSession.builder().master("local")
      .appName("SparkStatCleanJob").getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)

    //    accessRDD.take(10).foreach(println)

    // RDD ==》 DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

//    accessDF.printSchema()
    accessDF.show(false)

//    accessDF.write.format("parquet").partitionBy("day").save("hdfs://192.168.29.17:9000/sparkLogProgect/twicetLog")
    //通过 coalesce手动控制输出结果数量
    accessDF.coalesce(1).write.format("parquet").partitionBy("day")
      .save("hdfs://192.168.29.17:9000/sparkLogProgect/fourLog")

    spark.stop()
  }
}
