package com.bbd.FlinkTest.imooc

import java.io.File
import java.{lang, util}

import com.bbd.FlinkTest.dataSource.allDataSource.{numbers, parllwords, words}
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.JavaConverters._
import scala.util.Random



/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/2
  * Description: split datastream => splitstream
  *              sleect splitstream => datastream
  */
object DatastremaTranformation {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    demo1(env)
//    demo2(env)
//    paralldemo(env)
//    unionFunction(env)
    splitFunction(env)
    env.execute()
  }

  def demo1(env: StreamExecutionEnvironment): Unit ={
    //richSourceFunction类实现
    env.addSource(new words)
      .flatMap (_.split("-") filter {_.nonEmpty})
      .map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print()
  }

  def paralldemo(env: StreamExecutionEnvironment): Unit ={
    //ParallelSourceFunction 类实现 可并行收集多个
    env.addSource(new parllwords).setParallelism(5)
      .flatMap (_.split("-") filter {_.nonEmpty})
      .map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print()
  }

  def demo2(env: StreamExecutionEnvironment): Unit ={
    //SourceFunction 方法实现
    env.addSource(new SourceFunction[String] {
      var isrunning = true
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (isrunning){
          val tmp = (1 to 3)
          Thread.sleep(1000)
          val listwords = new util.ArrayList[String]()
          listwords.add("rand"+tmp(Random.nextInt(3)).toString)
          listwords.add("rand"+tmp(Random.nextInt(3)).toString)
          listwords.add("rand"+tmp(Random.nextInt(3)).toString)
          import scala.collection.JavaConverters._
          val rand_str = listwords.asScala.mkString("-")
          ctx.collect(rand_str)
        }
      }
      override def cancel(): Unit = isrunning = false
    }).flatMap (_.split("-") filter {_.nonEmpty})
      .map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print()
  }

  def unionFunction(env : StreamExecutionEnvironment): Unit ={
    val data1 = env.addSource(new words)
    val data2 = env.addSource(new words)
    data1.union(data2).print().setParallelism(1)
  }

  def splitFunction(env : StreamExecutionEnvironment): Unit ={
    // split datastream => splitstream
    // sleect splitstream => datastream
    val data = env.addSource(new numbers)
    val split_data = data.split((num: Int) =>{
      (num % 2) match {
        case 0 => List("even")
        case 1 => List("odd")
      }
    })

    val even_data = split_data select "even"
    val odd_data = split_data select "odd"
    val all_data = split_data select ("odd","even")

    even_data.print("even").setParallelism(1)
    odd_data.print("odd").setParallelism(1)
    all_data.print("all").setParallelism(1)

  }

  def splitFunction2(env : StreamExecutionEnvironment): Unit ={
    // split datastream => splitstream
    // sleect splitstream => datastream
    val data = env.addSource(new numbers)
    val split_data = data.split(new OutputSelector[Int] {
      override def select(value: Int): lang.Iterable[String] = {
        val tag = new util.ArrayList[String]()
        if (value % 2 == 1) tag.add("odd")
        else tag.add("even")
        tag
      }
    })

    val even_data = split_data select "even"
    val odd_data = split_data select "odd"
    even_data.print("even").setParallelism(1)
    odd_data.print("odd").setParallelism(1)

  }


}
