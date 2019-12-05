package com.bbd.FlinkTest.flinkSource

/**
  * @Author: maketubu
  * @Date: 2019/12/5 17:56
  */

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import com.bbd.FlinkTest.tools.randomTool
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object flinkCourseSourceCode4 {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new Datasource())

    stream.addSink(new SinkFunction[(String, Integer)] {
      override def invoke(value: (String, Integer), context: SinkFunction.Context[_]): Unit = {
        println(value._1, value._2)
      }
    })

    env.execute("source")
  }

  private class Datasource extends RichSourceFunction[Tuple2[String, Integer]]{

    private var runFlag = true
    override def run(ctx: SourceFunction.SourceContext[(String, Integer)]): Unit = {
      while (runFlag){
        Thread.sleep(1000)
        val data = randomTool.getRandomEmit
//        println(data)
        ctx.collect(data)
      }

    }
    override def cancel(): Unit = {
      runFlag = false
    }
  }
}
