package Log.dao

import java.sql.{Connection, PreparedStatement}

import Log.util.MySQLUtils

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的DAO操作
  */
object StatDAO {


  /**
    * 批量保存到数据库
    *
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false) //关闭自动提交

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (els <- list) {
        pstmt.setString(1, els.day)
        pstmt.setLong(2, els.cmsId)
        pstmt.setLong(3, els.times)

        pstmt.addBatch() //每次添加
      }

      pstmt.executeBatch() //一起执行 这样加快速度
      connection.commit() //手动提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }

  }

  def deleteDayData(day: String): Unit = {

    val tables = Array("day_video_access_topn_stat")

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MySQLUtils.getConnection()

      //      for(table <- tables){
      //        val sql = s"delete from $table where day = ?"
      //      }
      val deleteSql = "delete from day_video_access_topn_stat where day = ?"
      pstmt = connection.prepareStatement(deleteSql)
      pstmt.setString(1,day)
      pstmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
