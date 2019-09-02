import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SQLContext

object spark_sql {

  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/test/people.json"

    //1创建相应context

    val conf  =new SparkConf()
    conf.setAppName("context")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //2相关处理
    val people = sqlContext.read.format("json").load(inputPath)
    people.printSchema()
    people.show()
    //3关闭资源
    sc.stop()
  }
}
