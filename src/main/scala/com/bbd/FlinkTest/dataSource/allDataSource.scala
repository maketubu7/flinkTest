package com.bbd.FlinkTest.dataSource
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.bbd.FlinkTest.dao.caseClass.ServerMsg
import com.bbd.FlinkTest.dataSourceUtil.person
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import scala.collection.JavaConverters._
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
        val tmp = (1 to 3)
        Thread.sleep(1000)
        val listwords = new util.ArrayList[String]()
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        val rand_str = listwords.asScala.mkString("-") + "-"+count
        println("source: "+ rand_str)
        ctx.collect(rand_str)
        count += 1
        if (count > 100) running = false
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class parllwords extends ParallelSourceFunction[String] {

    private var running = true
    private var count = 0

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
        val tmp = (1 to 3)
        Thread.sleep(1000)
        val listwords = new util.ArrayList[String]()
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        import scala.collection.JavaConverters._
        val rand_str = listwords.asScala.mkString("-") + count
        ctx.collect(rand_str)
        count += 1
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class richparllwords extends RichParallelSourceFunction[String] {

    private var running = true
    private var count = 0

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
        val tmp = (1 to 3)
        Thread.sleep(1000)
        val listwords = new util.ArrayList[String]()
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        listwords.add("rand"+tmp(Random.nextInt(3)).toString)
        import scala.collection.JavaConverters._
        val rand_str = listwords.asScala.mkString("-") + count
        ctx.collect(rand_str)
        count += 1
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class numbers extends RichSourceFunction[Int] {

    private var running = true
    private var count = 0
    val ranodom = new Random()
    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
      while (running) {
        Thread.sleep(1000)
        val rand_int = ranodom.nextInt(99)
        println("source: "+ rand_int)
        ctx.collect(rand_int)
        count += 1
        if (count > 100) running = false
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class stduent_str extends RichSourceFunction[String] {

    private var running = true
    private var id = 0
    val random = new Random()
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
        Thread.sleep(1000)
        val xms = List("张三","李四","张二","李三","张五","李六")
        val xm = xms(random.nextInt(5))
        id += 1
        ctx.collect(s"$id,$xm")
        if (id > 1000)  running = false
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }


  class water_random_case extends RichSourceFunction[(Int,Long)] {

    private var running = true
    private var id = 0
    val random = new Random()

    override def run(ctx: SourceFunction.SourceContext[(Int,Long)]): Unit = {
      while (running) {
        Thread.sleep(1000)
        val event_time = System.currentTimeMillis() -random.nextInt(1)
        var key = 1
//        if (id % 2 == 0) key = 2
//        println("source "+ key.toString + " " + event_time.toString)
        ctx.collect((key,event_time))
        id += 1
        if (id > 100)  running = false
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class water_case extends RichSourceFunction[(Int,Long)] {

    private var running = true
    private var id = 0
    val cases = new util.ArrayList[(Int,Long)]()
    cases.add((1,1461756862000L))
    cases.add((1,1461756848000L))
    cases.add((1,1461756878000L))
    cases.add((1,1461756873000L))
    cases.add((1,1461756865000L))
    cases.add((1,1461756872000L))
    cases.add((1,1461756871000L))
    cases.add((1,1461756868000L))
    cases.add((1,1461756878000L))
    cases.add((1,1461756879000L))
    cases.add((1,1461756880000L))
    cases.add((1,1461756882000L))
    cases.add((1,1461756882000L))

    override def run(ctx: SourceFunction.SourceContext[(Int,Long)]): Unit = {
      while (running) {
        Thread.sleep(2000)
        ctx.collect(cases.get(id))
        Thread.sleep(4000)
        id += 1
        if (id > 12)  running = false
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
