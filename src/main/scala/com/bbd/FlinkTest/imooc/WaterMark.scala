package com.bbd.FlinkTest.imooc

import java.text.SimpleDateFormat

import com.bbd.FlinkTest.dataSource.allDataSource.{water_case, water_random_case}
import com.bbd.FlinkTest.tools.mysqlTool
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, TimestampAssigner}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/4
  * Description:
  */

/**对于watermark触发的条件 watermark时间 >= window_end_time 在[window_start_time,window_end_time)中有数据存在
  * 1、看得到第一条数据的event time 为 2016-04-27 19:34:22.000 窗口为 左闭右开[2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
  * 2、那么这个时间段是有数据存在的
  * 3、那么可以推测 第一条数据的触发时间是 timestamp 1461756864000|2016-04-27 19:34:24.000
  * 4、在第6条数据时，watermark已经来到了 1461756864000|2016-04-27 19:34:24.000
  * 5、所以触发了水位线，随之触发了计算结果
  * timestamp:1,1461756862000|2016-04-27 19:34:22.000,1461756862000|2016-04-27 19:34:22.000,Watermark @ -10000
  * timestamp:1,1461756866000|2016-04-27 19:34:26.000,1461756866000|2016-04-27 19:34:26.000,Watermark @ 1461756852000
  * timestamp:1,1461756872000|2016-04-27 19:34:32.000,1461756872000|2016-04-27 19:34:32.000,Watermark @ 1461756856000
  * timestamp:1,1461756873000|2016-04-27 19:34:33.000,1461756873000|2016-04-27 19:34:33.000,Watermark @ 1461756862000
  * timestamp:1,1461756874000|2016-04-27 19:34:34.000,1461756874000|2016-04-27 19:34:34.000,Watermark @ 1461756863000
  * (1,1,2016-04-27 19:34:22.000,2016-04-27 19:34:22.000,2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
  * timestamp:1,1461756875000|2016-04-27 19:34:35.000,1461756875000|2016-04-27 19:34:35.000,Watermark @ 1461756864000
  * timestamp:1,1461756876000|2016-04-27 19:34:36.000,1461756876000|2016-04-27 19:34:36.000,Watermark @ 1461756865000
  * timestamp:1,1461756877000|2016-04-27 19:34:37.000,1461756877000|2016-04-27 19:34:37.000,Watermark @ 1461756866000
  * (1,1,2016-04-27 19:34:26.000,2016-04-27 19:34:26.000,2016-04-27 19:34:24.000,2016-04-27 19:34:27.000)
  * timestamp:1,1461756878000|2016-04-27 19:34:38.000,1461756878000|2016-04-27 19:34:38.000,Watermark @ 1461756867000
*/

/*1、第一条数据 event time 为 2016-04-27 19:34:22.000 窗口为 左闭右开[2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
  2、输入第二条数据 为event time = 1461756875000 那么第三条数据的水位线就为 1461756862000 - 10 = 1461756865000 >= 2016-04-27 19:34:24.000
  3、所以第二条数据过后会马上触发水位线，随机触发计算
  timestamp:1,1461756862000|2016-04-27 19:34:22.000,1461756862000|2016-04-27 19:34:22.000,Watermark @ -10000
  timestamp:1,1461756875000|2016-04-27 19:34:35.000,1461756875000|2016-04-27 19:34:35.000,Watermark @ 1461756852000
  (1,1,2016-04-27 19:34:22.000,2016-04-27 19:34:22.000,2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
  timestamp:1,1461756878000|2016-04-27 19:34:38.000,1461756878000|2016-04-27 19:34:38.000,Watermark @ 1461756865000
*/

/* allowedLateness 允许的乱序时间+watermark允许的乱序时间位最大的舍弃时间late_time 当 watermark-window_end > late_time时
    可以使用sideOutputLateData进行侧输出流的接收
  1、第一条数据 event time 为 1461756862000|2016-04-27 19:34:22.000 窗口为 左闭右开[2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
  2、输入第二条数据 为event time = 1461756848000 那么第三条数据的水位线就为 1461756862000 - 10 = 1461756852000|2016-04-27 19:34:24.000 >= [2016-04-27 19:34:06.000,2016-04-27 19:34:09.000)
  3、所以第二条数据的水位线已经来到了2016-04-27 19:34:24.000，对于第二条数据的Event time watermark-window_end = 15 > 10 + 2 所以该条数据被舍弃
  4、舍弃的数据可以被侧输出流接收另作处理
  timestamp:1,1461756862000|2016-04-27 19:34:22.000,1461756862000|2016-04-27 19:34:22.000,Watermark @ -10000
  timestamp:1,1461756848000|2016-04-27 19:34:08.000,1461756862000|2016-04-27 19:34:22.000,Watermark @ 1461756852000
  output:6> (1,1461756848000,2016-04-27 19:34:08.000)  符合条件进入侧输出流
  timestamp:1,1461756878000|2016-04-27 19:34:38.000,1461756878000|2016-04-27 19:34:38.000,Watermark @ 1461756852000
  all:6> (1,1,2016-04-27 19:34:22.000,2016-04-27 19:34:22.000,2016-04-27 19:34:21.000,2016-04-27 19:34:24.000)
*/

object WaterMark {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    waterMarkFunction(senv)
//    waterMarkOutputFunction(senv)
    waterMarkTriggerFunction(senv)
    senv.execute()

  }

  def waterMarkFunction(senv: StreamExecutionEnvironment): Unit = {

    senv.addSource(new water_case).assignTimestampsAndWatermarks(new myAssignerWithPeriodicWatermarks)
      .keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest).print()
  }

  def waterMarkOutputFunction(senv: StreamExecutionEnvironment): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val side_out = new OutputTag[(Int, Long)]("late-out")
    val water: DataStream[(Int, Int, String, String, String, String)] = senv.addSource(new water_case)
      .assignTimestampsAndWatermarks(new myAssignerWithPeriodicWatermarks)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .allowedLateness(Time.seconds(2)).sideOutputLateData(side_out)
      .apply(new WindowFunctionTest)

    val output_stream = water.getSideOutput(side_out).map(x => {(x._1,x._2,format.format(x._2))})

    water.print("all")
    output_stream.print("output")
  }

  class WindowFunctionTest extends WindowFunction[(Int, Long), (Int, Int, String, String, String, String), Int, TimeWindow] {

    override def apply(key: Int, window: TimeWindow, input: Iterable[(Int, Long)],
                       out: Collector[(Int, Int, String, String, String, String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd))
    }
  }

  class myAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[(Int, Long)]{
    var currentMaxTimestamp = 0L
    val maxOutOfOrderness = 10000L //最大允许的乱序时间是10s
    var waterMark: Watermark = null
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def getCurrentWatermark: Watermark = {
      waterMark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      waterMark
    }

    override def extractTimestamp(t: (Int, Long), previousElementTimestamp: Long): Long = {
      val timestamp = t._2
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      val case_str = "timestamp:" + t._1 + "," + t._2 + "|" + format.format(t._2) + "," + currentMaxTimestamp + "|" +
        format.format(currentMaxTimestamp) + "," + waterMark.toString
      println(case_str)
      timestamp
    }
  }

  def waterMarkTriggerFunction(senv: StreamExecutionEnvironment): Unit = {
//    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val water = senv.addSource(new water_random_case)
      .assignTimestampsAndWatermarks(new myAssignerWithPeriodicWatermarks)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .trigger(new myTrigger(10))
      .apply(new WindowFunctionTest)

    water.print().setParallelism(1)


  }

  class myTrigger extends Trigger[(Int,Long), TimeWindow] {
    /* 可以自定义触发计算方式
    * 1、根据进入的event个数 进行触发
    * 2、根据定时器进行触发
    * 3、达到最大计算窗口进行触发计算
    *  */
    private var maxCount: Long = _
    //定时触发间隔时长 (ms)
//    private var interval: Long = 5 * 1000
    private var interval: Long = mysqlTool.getWindowTime * 1000
    //记录当前数量的状态
    private lazy val countStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("counter", new Sum, classOf[Long])
    //记录执行时间定时触发时间的状态
    private lazy val processTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("processTimer", new Update, classOf[Long])
    //记录事件时间定时器的状态
    private lazy val eventTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("eventTimer", new Update, classOf[Long])

    def this(maxCount: Int) {
      this()
      this.maxCount = maxCount
    }

    def this(maxCount: Int, interval: Long) {
      this(maxCount)
      this.interval = interval
    }

    override def onElement(element: (Int, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val countstate = ctx.getPartitionedState(countStateDescriptor)
      countstate.add(1L)

      if (ctx.getPartitionedState(eventTimerStateDescriptor).get() == 0L) {
        ctx.getPartitionedState(eventTimerStateDescriptor).add(window.maxTimestamp())
        ctx.registerEventTimeTimer(window.maxTimestamp())
      }

      if (countstate.get() >= this.maxCount) {
        //达到指定指定数量
        //删除事件时间定时触发的状态
        ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
        //清空计数状态
        countstate.clear()
        println(s"数量达到指定数量，执行计算 watermark: ${ctx.getCurrentWatermark}")
        //触发计算
        TriggerResult.FIRE
      } else if (ctx.getPartitionedState(processTimerStateDescriptor).get() == 0L) {
        //未达到指定数量，且没有指定定时器，需要指定定时器
        //当前定时器状态值加上间隔值
        println("interval:" + interval)
        ctx.getPartitionedState(processTimerStateDescriptor).add(ctx.getCurrentProcessingTime + interval)
        //注册定执行时间定时器
        ctx.registerProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
        TriggerResult.CONTINUE
      } else {
        TriggerResult.CONTINUE
      }

    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (ctx.getPartitionedState(countStateDescriptor).get() > 0 && (ctx.getPartitionedState(processTimerStateDescriptor).get() == time)) {
        println(s"数据量未达到 $maxCount ,由执行时间触发器 ctx.getPartitionedState(processTimerStateDescriptor).get()) 触发计算 $time")
        this.interval = mysqlTool.getWindowTime * 1000
        ctx.getPartitionedState(processTimerStateDescriptor).clear()
        ctx.getPartitionedState(countStateDescriptor).clear()
        TriggerResult.FIRE
      } else {
        TriggerResult.CONTINUE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      ctx.getCurrentWatermark
      if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDescriptor).get() > 0L)) { //还有未触发计算的数据
        println(s"事件时间到达最大的窗口时间，并且窗口中还有未计算的数据:${ctx.getPartitionedState(countStateDescriptor).get()}，" +
          s"触发计算并清除窗口[${window.getStart} : ${window.getEnd} : ${window.maxTimestamp()}) watermark: ${ctx.getCurrentWatermark}")
        ctx.getPartitionedState(eventTimerStateDescriptor).clear()
        TriggerResult.FIRE_AND_PURGE
      } else if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDescriptor).get() == 0L)) { //没有未触发计算的数据
        println("事件时间到达最大的窗口时间，但是窗口中没有有未计算的数据，清除窗口 但是不触发计算")
        TriggerResult.PURGE
      } else {
        TriggerResult.CONTINUE
      }
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(ctx.getPartitionedState(eventTimerStateDescriptor).get())
      ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
      ctx.getPartitionedState(countStateDescriptor).clear()
      ctx.getPartitionedState(eventTimerStateDescriptor).clear()
      ctx.getPartitionedState(processTimerStateDescriptor).clear()
      mysqlTool.release()
    }

    //更新状态为累加值
    class Sum extends ReduceFunction[Long] {
      override def reduce(value1: Long, value2: Long): Long = value1 + value2
    }

    //更新状态为取新的值
    class Update extends ReduceFunction[Long] {
      override def reduce(value1: Long, value2: Long): Long = value2
    }
  }

}
