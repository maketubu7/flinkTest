package com.bbd.FlinkTest.imooc

import java.text.SimpleDateFormat

import com.bbd.FlinkTest.dataSource.allDataSource.{water_case, water_random_case}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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

object WaterMark {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    waterMarkFunction(senv)
    senv.execute()

  }

  def waterMarkFunction(senv : StreamExecutionEnvironment): Unit = {
    senv.addSource(new water_case).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Int,Long)]{
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s
      var waterMark : Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        waterMark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        waterMark
      }

      override def extractTimestamp(t: (Int, Long), previousElementTimestamp: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val case_str = "timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+
              format.format(currentMaxTimestamp) + ","+ waterMark.toString
        println(case_str)
        timestamp
      }
    }).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunction[(Int,Long),(Int, Int,String,String,String,String),Int,TimeWindow]{
        override def apply(key: Int, window: TimeWindow, input: Iterable[(Int, Long)],
                           out: Collector[(Int, Int, String, String, String, String)]): Unit = {
          val list = input.toList.sortBy(_._2)
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))

        }
    }).print()

    class WindowFunctionTest extends WindowFunction[(Int,Long),(Int, Int,String,String,String,String),Int,TimeWindow]{

      override def apply(key: Int, window: TimeWindow, input: Iterable[(Int, Long)],
                         out: Collector[(Int, Int,String,String,String,String)]): Unit = {
        val list = input.toList.sortBy(_._2)
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
      }

    }

  }

}
