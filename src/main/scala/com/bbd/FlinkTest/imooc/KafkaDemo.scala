package com.bbd.FlinkTest.imooc

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

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
    kafkaProducerFunction(senv,"make_topic","make_out_topic")
    senv.execute()
  }

  def kafkaCousmerFunction(senv: StreamExecutionEnvironment,topic: String): Unit ={
    val props = new Properties()
    props.setProperty("bootstrap.servers", "192.168.89.135:9092")
    props.setProperty("group.id", "group1")
    val mycousmer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(),props)
    senv.addSource(mycousmer).print()

  }

  def kafkaProducerFunction(senv: StreamExecutionEnvironment,intopic: String, outtopic:String): Unit ={
    val inprops = new Properties()
    inprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    inprops.setProperty("group.id", "group1")
    val mycousmer = new FlinkKafkaConsumer[String](intopic, new SimpleStringSchema(),inprops)
    mycousmer.setStartFromLatest()
    val outprops = new Properties()
    outprops.setProperty("bootstrap.servers", "192.168.89.135:9092")
    val myproducer = new FlinkKafkaProducer[String](outtopic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      outprops,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    val random = new Random()
//    senv.enableCheckpointing(6000)
//    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    senv.getCheckpointConfig.setCheckpointTimeout(10000)
//    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(2)


    senv.addSource(mycousmer).map(x => {
      val tmp = JSON.parseObject(x)
      tmp.put("age",random.nextInt(40))
      tmp.toString
    }).addSink(myproducer)
  }
}
