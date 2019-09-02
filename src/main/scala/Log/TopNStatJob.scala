package Log

import Log.dao.{DayVideoAccessStat, StatDAO}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/sparkLogProgect/fourLog"

    val spark = SparkSession.builder().master("local")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false) //分区数据字段类型调整
      .appName("TopNStatJob").getOrCreate()

    val accessDF = spark.read.format("parquet").load(inputPath)

    //    accessDF.printSchema()
        accessDF.show(200)

    //最受欢迎的TopN课程
    vedioAccessTopNStat(spark, accessDF)
    spark.stop()
  }

  /**
    * 最受欢迎的Top课程，写到数据库
    *
    * @param spark
    * @param accessDF
    */
  def vedioAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    import spark.implicits._
    val vediosAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    vediosAccessTopNDF.show()

    try {
      //将统计结果写入到数据库
      vediosAccessTopNDF.foreachPartition(partitionOfRecord => {
        val list = new ListBuffer[DayVideoAccessStat]

        //将每个数据组装起来放在list中
        partitionOfRecord.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
