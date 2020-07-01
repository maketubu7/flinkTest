package com.bbd.FlinkTest.flinkSource

import java.text.SimpleDateFormat

import com.bbd.FlinkTest.dataSource.allDataSource.{tuple_str_str_source, words}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//引入隐式转换
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @Author: maketubu
  * @Date: 2020/5/26 16:15
  */
class someAPI{}

object someAPI {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    keybySumDemo(env)
//    keybyReduceDemo(env)
//      flatMapDemo(env)
    windowDemo(env)
  }

  def keybySumDemo(env:StreamExecutionEnvironment) : Unit = {
    //keybyed stream 按窗口求和
    val words: DataStream[String] = env.addSource(new words)
    val keyby_stream = words.map{(_,1)}.keyBy(0).timeWindow(Time.seconds(5)).sum(1).map{r => (r._1,r._2)}

    keyby_stream.print("sum: ")
    env.execute()
  }

  def keybyReduceDemo(env:StreamExecutionEnvironment) : Unit = {
    //keybyed stream 按窗口求和 reduce
    val words: DataStream[String] = env.addSource(new words)
    val keyby_stream: KeyedStream[(String, Int), Tuple] = words.map{(_,1)}.keyBy(0)
    keyby_stream.timeWindow(Time.seconds(50)).reduce{(t1:(String, Int),t2:(String, Int)) => {(t1._1,t1._2+t2._2)}}.print("reduce: ")
    env.execute()
  }

  def doublestr(x:String) : Array[(String,Int)] = {
    var tmp = Array[(String,Int)]()
    var tmp1 = tmp:+(x,1)
    var tmp2 = tmp1:+(x,1)
    tmp2
  }

  def doublestrAndTimeStamp(x:String) : Array[(String,Long)] = {
    var tmp = Array[(String,Long)]()
    var tmp1 = tmp:+(x,System.currentTimeMillis())
    var tmp2 = tmp1:+(x,System.currentTimeMillis())
    tmp2
  }

  def flatMapDemo(env:StreamExecutionEnvironment) : Unit = {
    //keybyed stream flatmap 窗口求和 sum
    val words: DataStream[String] = env.addSource(new words)
    val flatstream = words.flatMap{str => doublestr(str)}.keyBy(0).timeWindow(Time.seconds(3)).sum(1)
    flatstream.print("flatmap: ")
    env.execute()

  }

  def windowDemo(env: StreamExecutionEnvironment) :Unit = {
    val words: DataStream[String] = env.addSource(new words)

    /** 滚动窗口 TumblingEventTimeWindows 可以同时设置offset */
    words.flatMap{str => doublestrAndTimeStamp(str)}
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)]{
        var currentMaxTimestamp = 0L
        val maxOutOfOrderness = 5L//最大允许的乱序时间是5s
        var a : Watermark = null

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        override def getCurrentWatermark: Watermark = {
          println("change watermark")
          a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
          a
        }

        override def extractTimestamp(t: (String,Long), l: Long): Long = {
          val timestamp = t._2
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          println(t)
          if (a != null){
            println("timestamp:" + t._1 +","+ t._2 + "|" +","+  currentMaxTimestamp.toString + "|"+ ","+a.toString)
          }
//          println("timestamp:" + t._1 +","+ t._2 + "|" +","+  currentMaxTimestamp.toString + "|"+ ",")
          timestamp
        }
      })
      .keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .sum(1).print("TumblingEventTimeWindows: ")
    /** 滑动窗口 窗口的大小通过窗口大小参数指定，另外通过窗口slide参数控制滑动窗口的新建频率 大小10s，5s新建一个窗口 */
//    words.flatMap{str => doublestrAndTimeStamp(str)}
//        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)]{
//          var currentMaxTimestamp = 0L
//          val maxOutOfOrderness = 5L //最大允许的乱序时间是5s
//          var a : Watermark = null
//
//          override def getCurrentWatermark: Watermark = {
//            a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//            a
//          }
//
//          override def extractTimestamp(t: (String,Long), l: Long): Long = {
//            val timestamp = t._2
//            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//            println("timestamp:" + t._1 +","+ t._2 + "|"  +","+  currentMaxTimestamp.toString + "|"+ ",")
//            timestamp
//          }
//        })
//      .keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//      .sum(1).print("SlidingEventTimeWindows: ")
//    /** 会话窗口 分配器通过配置session gap来指定非活跃周期的时长,超过非活跃周期的即为一个新窗口 */
//    words.flatMap{str => doublestr(str)}.keyBy(0).window(EventTimeSessionWindows.withGap(Time.seconds(1)))
//      .sum(1).print("EventTimeSessionWindows: ")
//    /** 全局窗口 */
//    words.flatMap{str => doublestr(str)}.keyBy(0).window(GlobalWindows.create())
//      .sum(1).print("GlobalWindows: ")
    env.execute()
  }


}
