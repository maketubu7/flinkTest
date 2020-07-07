package com.bbd.FlinkTest.imooc

import com.bbd.FlinkTest.dataSource.allDataSource.stduent_str
import com.bbd.FlinkTest.tools.mysqlTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/2
  * Description: open/colse 生命周期方法
  *              invoke 每条记录实现一次
  */

object Datasetsink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    astextFunction(env)
    mysqlSinkFunction(senv)
    senv.execute("function")
  }

  def astextFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    val filepath1 = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data.txt"
    val filepath2 = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data"
    env.fromCollection(data).writeAsText(filepath1,writeMode = FileSystem.WriteMode.OVERWRITE)
    env.fromCollection(data).writeAsText(filepath2,writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(2)
  }

  def mysqlSinkFunction(senv: StreamExecutionEnvironment): Unit ={
    senv.addSource(new stduent_str).addSink(new RichSinkFunction[String] {
      override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
        //每条记录调用一次
        println("~~~~~invoke~~~~~~~")
        val data = value.split(",")
        val tuple_stdu = (data(0).toInt,data(1))
        mysqlTool.insertIntoStudetn(tuple_stdu)
      }

      override def open(parameters: Configuration): Unit = {
        //生命周期方法
        super.open(parameters)
        println("~~~~~core 并行度~~~~~~~")
      }

      override def close(): Unit = {
        mysqlTool.release()
      }
    })
  }
}
