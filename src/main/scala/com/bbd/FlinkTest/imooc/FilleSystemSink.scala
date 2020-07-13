package com.bbd.FlinkTest.imooc

import java.io.File
import java.time.ZoneId
import java.util

import com.bbd.FlinkTest.dataSource.allDataSource.words
import com.bbd.FlinkTest.imooc.DistributeCache.boardcastFunction
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.fs.{SequenceFileWriter, StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * Copyright@paidaxing
  * Author: paidaxing
  * Date:2020/7/8
  * Description:
  */
object FilleSystemSink {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //    distributeCacheFunction(env)
    fileSystemSink(senv)
    senv.execute()
  }

  def fileSystemSink(senv: StreamExecutionEnvironment): Unit = {
    val stream = senv.addSource(new words)
    stream.print()
    val filePath = "E:\\idea_workspace\\FlinkTest\\output\\scala\\hdfsSink"
    val fileSystemSink = new BucketingSink[String](filePath)
    fileSystemSink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHHmm"))
    fileSystemSink.setWriter(new StringWriter())
//    fileSystemSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    fileSystemSink.setBatchRolloverInterval(2000); // this is 20 mins
    stream.addSink(fileSystemSink).setParallelism(1)
  }

}
