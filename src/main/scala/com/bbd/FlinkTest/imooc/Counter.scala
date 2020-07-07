package com.bbd.FlinkTest.imooc

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/2
  * Description: step1 定义计数器
  *              step2 注册计数器
  *              step3 获取计数器
  */
object Counter {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    counterFunction(env)
  }

  def counterFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    val source = env.fromCollection(data).map(new RichMapFunction[Int,Int] {
      //step1 定义计数器
      val oddcountter = new LongCounter()
      val evencountter = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        //step2 注册计数器
        getRuntimeContext.addAccumulator("ele_odd_num",oddcountter)
        getRuntimeContext.addAccumulator("ele_even_num",evencountter)
      }
      override def map(in: Int): Int = {
        if (in % 2 == 1) oddcountter.add(1)
        else evencountter.add(1)
        in
      }
    })
    source.writeAsText("E:\\idea_workspace\\FlinkTest\\output\\scala\\out_counter").setParallelism(2)
    //step3 获取计数器结果
    val jobResult = env.execute("counterFunction")
    println("odd:"+jobResult.getAccumulatorResult("ele_odd_num"))
    println("even:"+ jobResult.getAccumulatorResult("ele_even_num"))
  }
}
