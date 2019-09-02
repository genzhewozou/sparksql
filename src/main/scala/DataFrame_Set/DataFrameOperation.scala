package DataFrame_Set

import org.apache.spark.sql.SparkSession

object DataFrameOperation {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("operation").getOrCreate()

    val inputPath = "hdfs://192.168.29.17:9000/test/student.data"

    val studentRDD = sparkSession.sparkContext.textFile(inputPath)

    import sparkSession.implicits._
    val studentDF = studentRDD.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //默认只显示前20条，并且，当超过一定长度后，会进行截取
    studentDF.show()
    studentDF.show(30, false)

    studentDF.take(10)//前10条

    //过滤名字为空或者为NULL
    studentDF.filter("name='' OR name='NULL'").show(30, false)

    //name以M开头的
    studentDF.filter("SUBSTR(name,0,1)='M'").show()

    //排序
    studentDF.sort(studentDF("name")).show()
    studentDF.sort(studentDF("name").desc).show() //降序
    studentDF.sort("name","id").show()
    studentDF.sort(studentDF("name").asc,studentDF("id").desc).show() //名字升序，id降序

    //两表连接，注意3个等号
    val studentDF2 = studentRDD.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studentDF.join(studentDF2,studentDF.col("id") === studentDF2.col("id"),"inner").show()

    sparkSession.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
