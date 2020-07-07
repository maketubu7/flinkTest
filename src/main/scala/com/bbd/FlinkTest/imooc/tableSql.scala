package com.bbd.FlinkTest.imooc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/2
  * Description:
  */
object tableSql {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    tableSqlFunction(env)
  }

  def tableSqlFunction(env: ExecutionEnvironment): Unit ={
    // 获取table执行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data.txt"
    val dataset: DataSet[ids] = env.readTextFile(filePath).map(ids)
    //创建一张表
    val ids_table = tableEnv.fromDataSet(dataset)
    //注册临时表
    tableEnv.registerTable("ids_table",ids_table)
    //sql 查询
    val sql_res: Table = tableEnv.sqlQuery("select id from ids_table order by id")
    tableEnv.toDataSet[Row](sql_res).print()

  }

  case class ids (id: String)
  case class res (sum: BigInt)
}
