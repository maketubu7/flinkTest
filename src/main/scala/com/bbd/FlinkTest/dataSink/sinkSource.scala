package com.bbd.FlinkTest.dataSink

import com.bbd.FlinkTest.tools.mysqlTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable

/**
  * @Author: maketubu
  * @Date: 2019/12/9 20:13
  */
object sinkSource {
  /** 自定义sink 总和计入结果 */
  class allCountSink extends RichSinkFunction[mutable.HashMap[String, Int]] {

    override def open(parameters: Configuration): Unit = super.open(parameters)

    //value 输入类型
    override def invoke(value: mutable.HashMap[String, Int], context: SinkFunction.Context[_]): Unit = {
      val sum = value.values.sum
      mysqlTool.updateItemSum(sum)
      println("allItemCount Update Success: %d".format(sum))
    }

    override def close(): Unit = super.close()
  }

  /** 自定义sink 分组个数计入结果 */
  class groupCountSink extends RichSinkFunction[(String, Int)] {

    override def open(parameters: Configuration): Unit = super.open(parameters)

    //value 输入类型
    override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
      mysqlTool.updateItemGroupSum(value)
      println("groupItemCount Update Success: %s-%d".format(value._1, value._2))
    }

    override def close(): Unit = {
      mysqlTool.release()
    }
  }
}
