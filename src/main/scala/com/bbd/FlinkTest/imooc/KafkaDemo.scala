package com.bbd.FlinkTest.imooc

import java.util.Properties
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.bbd.FlinkTest.tools.mysqlTool
import com.bbd.FlinkTest.tools.mysqlTool.conn
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/9
  * Description:
  */
object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //    kafkaCousmerFunction(senv,"make_topic")
//    kafkaProducerFunction(senv, "make_topic", "make_out_topic")
    kafkaMysqlSinkFunction(senv, "make_topic")
    senv.execute()
  }

  def kafkaCousmerFunction(senv: StreamExecutionEnvironment, topic: String): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "192.168.89.135:9092")
    props.setProperty("group.id", "group1")
    val mycousmer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)
    senv.addSource(mycousmer).print()

  }

  def kafkaProducerFunction(senv: StreamExecutionEnvironment, intopic: String, outtopic: String): Unit = {
    val inprops = new Properties()
    inprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    inprops.setProperty("group.id", "group1")
    val mycousmer = new FlinkKafkaConsumer[String](intopic, new SimpleStringSchema(), inprops)
    mycousmer.setStartFromLatest()
    val outprops = new Properties()
    outprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    val myproducer = new FlinkKafkaProducer[String](outtopic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      outprops, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    val random = new Random()
    //    senv.enableCheckpointing(6000)
    //    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    senv.getCheckpointConfig.setCheckpointTimeout(10000)
    //    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    senv.addSource(mycousmer).map(x => {
      val tmp = JSON.parseObject(x)
      tmp.put("age", random.nextInt(40))
      println(tmp.toString)
      tmp.toString
    }).addSink(myproducer)
  }


  def kafkaMysqlSinkFunction(senv: StreamExecutionEnvironment, intopic: String): Unit = {
    val inprops = new Properties()
    inprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    inprops.setProperty("group.id", "group1")
    val mycousmer = new FlinkKafkaConsumer[String](intopic, new SimpleStringSchema(), inprops)
    mycousmer.setStartFromLatest()
    val outprops = new Properties()
    outprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    val random = new Random()
    senv.addSource(mycousmer).map(x => {
      val tmp = JSON.parseObject(x)
      tmp.put("age", random.nextInt(40))
      tmp.toString
    }).addSink(new RichSinkFunction[String] {
      var mysqlService: mysqlTool.type = null
      var count = 0
      var kafkaBuffer: ArrayBuffer[String] = ArrayBuffer[String]()
      override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
        println(s"invoke $value")
        kafkaBuffer.append(value)
        count += 1
        if (count % 100 == 0) {
          mysqlService.insertBatchKakfa(kafkaBuffer)
          println("execute success")
          kafkaBuffer.clear()
        }
      }

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        mysqlService = mysqlTool
        mysqlService.getConnection()
      }

      override def close(): Unit = {
        println("invoke release")
        mysqlService.insertBatchKakfa(kafkaBuffer)
        mysqlTool.release()
      }
    })
  }
}
