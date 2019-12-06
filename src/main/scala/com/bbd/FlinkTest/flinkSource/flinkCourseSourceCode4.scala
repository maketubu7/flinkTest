package com.bbd.FlinkTest.flinkSource

/**
  * @Author: maketubu
  * @Date: 2019/12/5 17:56
  */

import java.util.Random

import com.bbd.FlinkTest.tools.mysqlTool
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
import scala.collection.mutable

class flinkCourseSourceCode4{}

object flinkCourseSourceCode4 {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(classOf[flinkCourseSourceCode4])
    import org.apache.flink.streaming.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[(String, Int)] = env.addSource(new Datasource())

    //    stream.addSink(new SinkFunction[(String, Int)] {
    //      override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //        println("get" ,value._1, value._2)
    //      }})

        stream.keyBy(0).timeWindow(Time.seconds(5))
          .reduce((t1:(String, Int), t2:(String, Int)) => {
          val value = t1._2 + t2._2
          (t1._1,value)
        }).addSink(new groupCountSink)

//    stream.keyBy(0).sum(1).keyBy(r => {""}).timeWindow(Time.seconds(5)).fold(new mutable.HashMap[String, Int]())(
//      (accumulator, value) => {
//        accumulator.put(value._1, value._2)
//        accumulator
//      }
//    ).addSink(new SinkFunction[mutable.HashMap[String, Int]] {
//      override def invoke(value: mutable.HashMap[String, Int], context: SinkFunction.Context[_]): Unit = {
//        val sum = value.values.sum
//        println("outAllCount:", sum)
//      }
//    })

//    stream.keyBy(0).sum(1).keyBy(r => {""}).timeWindow(Time.seconds(5)).fold(new mutable.HashMap[String, Int]())(
//      (accumulator, value) => {
//        accumulator.put(value._1, value._2)
//        accumulator
//      }
//    ).addSink(new allCountSink)


    env.execute("source")
  }

  /** 自定义source */
  private class Datasource extends RichSourceFunction[(String, Int)] {

    private var running = true

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      val random = new Random()
      while (running) {
        Thread.sleep(1000)
        val key = "类别" + ('A' + random.nextInt(4)).toChar
        val value = random.nextInt(10) + 1
        println("put: ", key, value)
        ctx.collect((key, value))
      }

    }

    override def cancel(): Unit = {
      running = false
    }
  }


  /** 自定义sink 总和计入结果 */
  private class allCountSink extends RichSinkFunction[mutable.HashMap[String, Int]] {

    override def open(parameters: Configuration): Unit = super.open(parameters)

    //value 输入类型
    override def invoke(value: mutable.HashMap[String, Int], context: SinkFunction.Context[_]): Unit = {
      val sum = value.values.sum
      mysqlTool.updateItemSum(sum)
      println("allItemCount Update Success: %d".format(sum))
    }

    override def close(): Unit = super.close()
  }

  /** 自定义sink 分组个数计入结果 */
  private class groupCountSink extends RichSinkFunction[(String, Int)] {

    override def open(parameters: Configuration): Unit = super.open(parameters)

    //value 输入类型
    override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
      mysqlTool.updateItemGroupSum(value)
      println("groupItemCount Update Success: %s-%d".format(value._1, value._2))
    }

    override def close(): Unit = {
      mysqlTool.release()
    }
  }

}
