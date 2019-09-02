package DataFrame_Set

import org.apache.spark.sql.SparkSession

object SparkSqlUseMysql {

  def main(args: Array[String]): Unit = {

    val sparklSession = SparkSession.builder().master("local")
      .appName("hivetest")
      .enableHiveSupport().getOrCreate()

    val mysql = sparklSession.read.format("jdbc").option("url","jdbc:mysql://192.168.29.17:3306/hive").
      option("dbtable","hive.TBLS").option("driver","com.mysql.jdbc.Driver").
      option("user","root").option("password","Woaini,1996").load()

    sparklSession.stop()
  }
}
