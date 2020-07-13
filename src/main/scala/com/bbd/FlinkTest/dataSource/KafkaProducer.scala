package com.bbd.FlinkTest.dataSource

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/9
  * Description:
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    producerFunction("make_topic",100)
  }
  def producerFunction(topic: String,num : Int): Unit ={
    val brokers_list = "192.168.89.135:9092"
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
    val producer = new KafkaProducer[String, String](properties)
    for(i<- 1 to num){
      val json = new JSONObject()
      json.put("name","jason"+i)
      json.put("addr","25"+i)
      producer.send(new ProducerRecord(topic,json.toString()))
      println(json.toString())
    }
    producer.close()
  }

}
