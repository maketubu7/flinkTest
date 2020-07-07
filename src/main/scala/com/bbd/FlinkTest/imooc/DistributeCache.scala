package com.bbd.FlinkTest.imooc

import java.io.File
import java.util
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/2
  * Description: step1： 注册分布式缓存
  *              step2: 读取分布式缓存
  *              step3: 使用分布式缓存
  */
object DistributeCache {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    distributeCacheFunction(env)
    boardcastFunction(env)
  }

  def distributeCacheFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    val filePath = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data.txt"
    env.registerCachedFile(filePath,"one-ten-data")
    env.fromCollection(data).map(new RichMapFunction[Int,Int] {
      var word_top: Int = 0
      override def open(parameters: Configuration): Unit = {
        val cacheFile: File = getRuntimeContext.getDistributedCache.getFile("one-ten-data")
        val datas: util.List[String] = FileUtils.readLines(cacheFile)
        for (word <- datas.asScala) println(word)
        word_top = datas.asScala(2).toInt
        println(s"word_top: $word_top")
      }
      override def map(value: Int): Int = {
        value + word_top
      }
    }).print()
  }

  def boardcastFunction(env: ExecutionEnvironment): Unit ={
    val boardSet = env.fromElements(1,2,3)
    val data = 1 to 10
    var word_sum = 0
    env.fromCollection(data).map(new RichMapFunction[Int,Int] {
      var broadcastSet: Traversable[Int] = _
      override def open(parameters: Configuration): Unit = {
        broadcastSet = getRuntimeContext.getBroadcastVariable("boardname").asScala
        for (v <- broadcastSet) println(v)
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        word_sum = broadcastSet.toList.sum
      }

      override def map(value: Int): Int = {
        value + word_sum
      }
    }).withBroadcastSet(boardSet,"boardname").print()
  }

}
