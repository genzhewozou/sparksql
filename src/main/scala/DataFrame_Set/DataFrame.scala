package DataFrame_Set

import org.apache.spark.sql.SparkSession

/*
DataFrame的基本api操作
 */
object DataFrame {

  def main(args: Array[String]): Unit = {

    val inputPath = "hdfs://192.168.29.17:9000/test/people.json"

    val sparklSession = SparkSession.builder().master("local")
      .appName("hivetest")
      .getOrCreate()

    val people = sparklSession.read.format("json").load(inputPath)

    //输出dataframe对应的schema信息
    people.printSchema()

    //输出数据集的前20条记录，可以在括号内写数字指定输出数量
    people.show()

    //查询某列所有数据  select name from table
    people.select("name").show()

    //查询某几列数据，并且对数据进行计算 select name,age+10 from table
    people.select(people.col("name"),people.col("age")+10).show()

    //查询某几列数据，并且对数据进行计算,并且起一个别名 select name,age+10 from table
    people.select(people.col("name"),(people.col("age")+10).as("age2")).show()

    //查询某一列，并且进行条件过滤 select age from table where age >19
    people.filter(people.col("age")>19).show()

    //根据某一列进行分组，随后聚合 select age,count(1) from table group by age
    people.groupBy(people.col("age")).count().show()

    sparklSession.stop()
  }
}
