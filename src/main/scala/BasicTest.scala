object BasicTest {

  def main(args: Array[String]): Unit = {
    var a = 1

    var strings =
      """你好
        |臭
        |弟弟
      """.stripMargin

    var b = 0
    for (b <- 3 to 10) {
      //        println(b)
    }

    val c = 0
    val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    for (c <- numList
         if c != 3; if c < 8) {
      //        println(c)
    }

    def addi(a: Int, b: Int): Int = {
      var sum = 0
      sum = a + b
      return sum
    }

    var buf = new StringBuilder
    buf += 'a'
    buf ++= "bcdef"
    var but = "vv"
    var len = buf.length() //字符串长度
    //      println(buf + len.toString + buf.concat(but))

    var ary = new Array[String](3)
    ary(0) = "a"
    var ary1 = Array(1, 2, 3)
    for (x <- ary1) {
      //        println(x)
    }

    val xlist = List(1, 2, 3, 4)
    val xmap = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val xarray = (10, "Runoob")
    println(xmap.get("two"))

    for (m <- xmap) {
      //        println(m._1,m._2)
    }
    for (n <- xarray) {
      println(n)
    }

  }
}

