package com.bbd.FlinkTest.flinkSource
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

import scala.collection.mutable.ListBuffer

object Datasetsink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    astextFunction(env)
    env.execute("function")
  }

  def astextFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    val filepath1 = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data.txt"
    val filepath2 = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data"
    env.fromCollection(data).writeAsText(filepath1,writeMode = FileSystem.WriteMode.OVERWRITE)
    env.fromCollection(data).writeAsText(filepath2,writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(2)
  }
}
