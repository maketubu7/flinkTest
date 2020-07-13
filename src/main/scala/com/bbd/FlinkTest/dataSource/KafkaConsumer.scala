package com.bbd.FlinkTest.dataSource

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/9
  * Description:
  */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    cunsumerFunction("make_topic")
  }

  def cunsumerFunction(topic: String): Unit = {
    val props = new Properties()
    val brokers_list = "192.168.89.137:9092"
    props.put("bootstrap.servers", brokers_list)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topic))
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(1)
      for (record <- records){
        println(record.key() + ": " + record.value())
      }
    }
    consumer.close()
  }
}
