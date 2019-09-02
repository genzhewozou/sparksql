import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test_hive {

  def main(args: Array[String]): Unit = {
//        val conf = new SparkConf()
//        conf.setAppName("WordCount").setMaster("local")
//        val hive = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
//        hive.sql("show tables").show()



    val sparklSession = SparkSession.builder().master("local")
      .appName("hivetest")
      .enableHiveSupport().getOrCreate()
    sparklSession.sql("show tables").show()
  }
}
