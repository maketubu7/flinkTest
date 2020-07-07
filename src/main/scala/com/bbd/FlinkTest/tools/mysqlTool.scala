package com.bbd.FlinkTest.tools

/**
  * @Author: maketubu
  * @Date: 2019/12/6 11:32
  */

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  */
class mysqlTool{}
object mysqlTool {

  var conn: Connection = _

  def updateItemSum(value: Int): Unit ={
    getConnection()
    val updateSql = "update itemAllcount set all_item_count = %d".format(value)
    conn.setAutoCommit(true)
    val ps = conn.prepareStatement(updateSql)
    ps.execute()
  }

  def updateItemGroupSum(value: (String, Int)): Unit ={
    getConnection()
    val updateSql = ("INSERT into itemgroupcount(item_name, item_count) " +
                  "VALUES ('%s', %d) ON DUPLICATE KEY UPDATE item_count = %d").format(value._1, value._2, value._2)
    conn.setAutoCommit(true)
    val ps = conn.prepareStatement(updateSql)
    ps.execute()
  }

  def insertIntoStudetn(value: (Int,String)): Unit ={
    getConnection()
    val updateSql = ("INSERT into student(id, name) " +
      "VALUES (%d, '%s') ON DUPLICATE KEY UPDATE id = %d").format(value._1, value._2,value._1)
    conn.setAutoCommit(true)
    val ps = conn.prepareStatement(updateSql)
    ps.execute()
  }

  /**
    * 获取数据库连接
    */
  def getConnection(): Unit = {

    val jdbc = PropertiesTool.getproperties("jdbc","mysql.properties")
    val user = PropertiesTool.getproperties("user","mysql.properties")
    val password = PropertiesTool.getproperties("passwd","mysql.properties")
    conn = DriverManager.getConnection(jdbc, user,password)

  }

  /**
    * 释放数据库连接等资源
    */
  def release(): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
        print("")
      }
    }


  def main(args: Array[String]) {
//    updateItemSum(67)
    updateItemGroupSum(("类别C",89))
  }

}
