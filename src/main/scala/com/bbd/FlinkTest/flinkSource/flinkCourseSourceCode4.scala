package com.bbd.FlinkTest.flinkSource

/**
  * @Author: maketubu
  * @Date: 2019/12/5 17:56
  */

import java.util.Random

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object flinkCourseSourceCode4 {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[(String, Int)] = env.addSource(new Datasource())

//    stream.addSink(new SinkFunction[(String, Int)] {
//      override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
//        println("sink" ,value._1, value._2)
//      }})

    stream.keyBy(0).timeWindow(Time.seconds(5))
      .reduce((t1:(String, Int), t2:(String, Int)) => {
      val value = t1._2 + t2._2
      (t1._1,value)
    }).print("out: ")

    env.execute("source")
  }

  private class Datasource extends RichSourceFunction[(String, Int)]{

    private var runFlag = true

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      val random = new Random()
      while (runFlag){
        Thread.sleep(1000)
        val key = "类别" + ('A' + random.nextInt(3)).toChar
        val value = random.nextInt(10) + 1
        println("put: ",key, value)
        ctx.collect((key, value))
      }

    }
    override def cancel(): Unit = {
      runFlag = false
    }
  }
}
