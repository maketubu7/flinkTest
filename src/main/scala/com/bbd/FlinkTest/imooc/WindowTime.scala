package com.bbd.FlinkTest.imooc

import com.bbd.FlinkTest.dataSource.allDataSource.numbers
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/4
  * Description: reduceFunction
  */
object WindowTime {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    reduceFunction(senv)
    processFunction(senv)
    senv.execute()
  }

  def reduceFunction(senv: StreamExecutionEnvironment): Unit ={
    senv.addSource(new numbers).map(x => {
      var key = 1
      if (x % 2==0) key = 2
      (key,x)
    })
  }.keyBy(0).timeWindow(Time.seconds(5))
    .reduce((x1,x2) => {  ///增量两两相加，不必等到窗口结束
    println(x1._1,x1._2,"+",x2._2)
    (x1._1,x1._2+x2._2)
  }).print()


  def processFunction(senv: StreamExecutionEnvironment): Unit ={
    ///窗口结束后。在统计每个窗口，会增加资源的消耗
    senv.addSource(new numbers).map(x => {
      var key = 1
      if (x % 2==0) key = 2
      (key,x)
    })
  }.keyBy(0).timeWindow(Time.seconds(5))
    .process(new ProcessWindowFunction[(Int, Int), String, Tuple, TimeWindow]() {
    override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[String]): Unit = {
      var count = 0
      for (v <- elements){
        count += 1
      }
      out.collect("window " + context.window + " count "+ count.toString)
    }
  }).print()
}
