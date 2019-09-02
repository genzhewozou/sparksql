package DataFrame_Set

import org.apache.spark.sql.SparkSession

object DataSetAPP {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("operation").getOrCreate()

    val inputPath = "hdfs://192.168.29.17:9000/test/sales.csv"

    //spark解析csv文件,直接返回DataFrame
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(inputPath)

    import sparkSession.implicits._
    val ds = df.as[Scals]
    ds.map(line =>line.itemId).show()

    sparkSession.stop()
  }

  case class Scals(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}
