//import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {

    val inputfile = "hdfs://192.168.29.17:9000/mt.txt"


        val conf = new SparkConf() //创建spark配置对象
          .setAppName("test")
          .setMaster("local") //本地代表为测试，可以放到spark上集群上跑
        //        .setMaster("spark://192.168.29.17:7077")

        val sc = new SparkContext(conf) //通过conf创建sc

//        val sqlcontext = new SQLContext(sc); //构建sparkSql

        val textFile = sc.textFile(inputfile)
        textFile.foreach(println)
        sc.stop()


    /*
  这是供给打包使用的，单纯的打包到linux上运行
   */
    //  def main(args: Array[String]): Unit = {
    //    //创建spark配置对象
    //    val conf = new SparkConf()
    //    //通过conf创建sc
    //    val sc = new SparkContext(conf)
    //    //书写单词计数程序
    //    val textFile = sc.textFile(args(0))
    //    textFile.foreach(println)
    //  }
  }
}
