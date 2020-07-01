package com.bbd.FlinkTest.flinkSource

/**
  * @Author: maketubu
  * @Date: 2019/12/5 17:56
  */

import java.util.Random

import com.bbd.FlinkTest.dao.caseClass.ServerMsg
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import com.bbd.FlinkTest.dataSource.allDataSource._

import scala.collection.mutable

class flinkCourseSourceCode4{}

object flinkCourseSourceCode4 {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val logger = Logger.getLogger(classOf[flinkCourseSourceCode4])

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val tuplestream: DataStream[(String, Int)] = env.addSource(new Datasource())
    val tuple_str_str_stream: DataStream[(String, String)] = env.addSource(new tuple_str_str_source)
//    val serverstream = env.addSource(new Serversource)

    //    stream.addSink(new SinkFunction[(String, Int)] {
    //      override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //        println("get" ,value._1, value._2)
    //      }})

//        stream.keyBy(0).timeWindow(Time.seconds(5))
//          .reduce((t1:(String, Int), t2:(String, Int)) => {
//          val value = t1._2 + t2._2
//          (t1._1,value)
//        }).addSink(new groupCountSink)

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

//    val outputTag = OutputTag[(String, Int)]("side-output")
//    val outputstream: DataStream[(String, Int)] = stream.keyBy(0).process(new ProcessFunction[(String, Int),(String, Int)](){
//      override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int),(String, Int)]#Context, collector: Collector[(String, Int)]): Unit = {
//        if (value._1.contains("A")){
//          /** 根据条件划分侧输出流 */
//          ctx.output(outputTag,("side_out" + value._1, value._2))
//        }
//        else{
//          /** 不满足条件源流收集 */
//          collector.collect(value)
//        }
//      }
//    } )
//
//    outputstream.print("all: ")
//    outputstream.getSideOutput(outputTag).print("side_out: ")

//    serverstream.keyBy(_.serverId).process(new MonitorKeyedProcessFunction).print("get: ")
//    tuple_str_str_stream.keyBy(0).process(new CountWithTimeoutFunction).print("out: ")

    env.execute("source")
  }

  case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

  class MonitorKeyedProcessFunction extends KeyedProcessFunction[String, ServerMsg, String] {

    private var timeState: ValueState[Long] = _
    private var serverState: ValueState[String] = _



    override def open(parameters: Configuration): Unit = {
      timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", TypeInformation.of(classOf[Long])))
      serverState = getRuntimeContext.getState(new ValueStateDescriptor[String]("server-state", TypeInformation.of(classOf[String])))
    }



    override def processElement(value: ServerMsg, ctx: KeyedProcessFunction[String, ServerMsg, String]#Context, out: Collector[String]): Unit = {
      if (!value.isOnline) {
        val monitorTime = ctx.timerService().currentProcessingTime() + 30000

        timeState.update(monitorTime)
        serverState.update(value.serverId)

        ctx.timerService().registerProcessingTimeTimer(monitorTime)
      }

      if (value.isOnline && -1 != timeState.value()) {
        ctx.timerService().deleteProcessingTimeTimer(timeState.value())
        timeState.update(-1)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ServerMsg, String]#OnTimerContext, out: Collector[String]): Unit = {

      if (timestamp == timeState.value()) {
        val newMonitorTime = timestamp + 30000

        timeState.update(newMonitorTime)
        ctx.timerService().registerProcessingTimeTimer(newMonitorTime)

        println("告警:" + serverState.value() + " is offline, please restart")
      }
    }
  }
  class CountWithTimeoutFunction extends ProcessFunction[(String, String), (String, Long)] {

    /** The state that is maintained by this process function */
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
      .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


    override def processElement(value: (String, String), ctx: ProcessFunction[(String, String), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      // initialize or retrieve/update the state

      var current: CountWithTimestamp = state.value
      print("tag: ", value._1, 1, ctx.timestamp())

      if (current == null){
        current = CountWithTimestamp(value._1, 1, System.currentTimeMillis())
      }
      else{
        current = CountWithTimestamp(current.key, current.count+1, ctx.timestamp)
      }

      // write the state back
      state.update(current)

      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
    }

    override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
      state.value match {
        case CountWithTimestamp(key, count, lastModified) if timestamp == lastModified + 60000 =>
          out.collect((key, count))
        case _ =>
      }
    }
  }

}
