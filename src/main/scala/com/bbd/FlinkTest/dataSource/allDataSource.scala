package com.bbd.FlinkTest.dataSource
import java.text.SimpleDateFormat
import java.util.Calendar

import com.bbd.FlinkTest.dao.caseClass.ServerMsg
import com.bbd.FlinkTest.dataSourceUtil.person
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @Author: maketubu
  * @Date: 2019/12/9 20:05
  */

object allDataSource {

  class tuple_str_int_source extends RichSourceFunction[(String, Int)] {

    private var running = true

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      val random = new Random()
      while (running) {
        Thread.sleep(1000)
        val key = "类别" + ('A' + random.nextInt(4)).toChar
        val value = random.nextInt(10) + 1
        //        val time = System.currentTimeMillis()
        println("put: ", key, value)
        ctx.collect((key, value))
      }

    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class tuple_str_str_source extends RichSourceFunction[(String, String)] {

    private var running = true

    override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
      val random = new Random()
      while (running) {
        Thread.sleep(1000)
        val key = "类别" + ('A' + random.nextInt(4)).toChar
        val value = (random.nextInt(10) + 1).toString
        //        val time = System.currentTimeMillis()
        println("put: ", key, value)
        ctx.collect((key, value))
      }

    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class Serversource extends RichSourceFunction[ServerMsg] {

    private var running = true

    override def run(ctx: SourceFunction.SourceContext[ServerMsg]): Unit = {
      val random = new Random()
      val start_time = System.currentTimeMillis()
      while (running) {
        Thread.sleep(1000)
        val serverId = "bbd0" + random.nextInt(4).toString
        var isOnline = false
        val timestamp = System.currentTimeMillis()
        if (timestamp - start_time > 40000){
          isOnline = true
        }
        println("put: ", serverId, isOnline,timestamp)
        ctx.collect(ServerMsg(serverId, isOnline,timestamp))
      }

    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class person_source extends RichSourceFunction[String] {

    private var running = true
    private var count = 0

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
//        Thread.sleep(1000)
        val sfzh = person.get_sfzh()
        count = count + 1
        if (count > 100) cancel()
        ctx.collect(sfzh)

      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class words extends RichSourceFunction[String] {

    private var running = true
    private var count = 0

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
        val tmp = (1 to 5)
        Thread.sleep(1000)
        val str: String = tmp(Random.nextInt(5)).toString
//        println(str)
        ctx.collect(str)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }
}
