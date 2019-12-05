package com.bbd.FlinkTest.flinkSource

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector



/**
  * @Author: maketubu
  * @Date: 2019/12/3 17:47
  */
object simpleProcessDemo {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    //        import org.apache.flink.api.scala._
    // windowTime设置窗口时间大小，默认2分钟一个窗口足够读取文本内的所有数据了
    val windowTime = 2

    // 构造执行环境，使用eventTime处理窗口数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setEnv = ExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val FilePath = "E:\\idea_workspace\\FlinkTest\\src\\main\\resources\\commonSource.txt"

    //    add comment make it lazy
    val stream: DataStream[String] = env.readTextFile(FilePath)
    val set: DataSet[String] = setEnv.readTextFile(FilePath)


    val peopleStream = stream.map(r => getPeople(r))
    val userStream = stream.map(r => getRecord(r))

    val connect: ConnectedStreams[People, UserRecord] = peopleStream.connect(userStream)

    peopleStream.map(r => (r.name, r.sexy)).keyBy(0).process(new CountWithTimeoutFunction()).print()


    env.execute()
    //    setEnv.execute()
  }
  def getRecord(line: String): UserRecord = {
    val elems = line.split(" ")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  def getPeople(line: String): People = {
    val elems = line.split(" ")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    People(name, sexy, time)
  }


  // UserRecord数据结构的定义
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)
  case class People(name: String, sexy: String, shoppingTime: Int)

  /**
    *  state中保存的数据类型
    */
  /**
    * ProcessFunction的实现，用来维护计数和超时
    */
  case class CountWithTimestamp(sexy: String, count: Int, lastModified: Long)

  /**
    * The implementation of the ProcessFunction that maintains the count and timeouts
    */
  class CountWithTimeoutFunction extends ProcessFunction[(String, String), (String, Long)] {

    /** The state that is maintained by this process function */
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


    override def processElement(value: (String, String), ctx: ProcessFunction[(String, String),(String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      /** 初始化或者获取/更新状态 */

      /** process function维持的状态  */

      /** 定时器允许应用程序对processing time和event time的变化做出反应，
      每次对processElement(...)的调用都会得到一个Context对象，
      该对象允许访问元素事件时间的时间戳和TimeServer。
      TimeServer可以用来为尚未发生的event-time或者processing-time注册回调，
      当定时器的时间到达时，onTimer(...)方法会被调用。在这个调用期间，
      所有的状态都会限定到创建定时器的键，并允许定时器操纵键控状态(keyed states) */


      val current: CountWithTimestamp = state.value match {
        case null =>
          CountWithTimestamp(value._2, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }

      // 回写state状态
      state.update(current)

      /** 从当前事件时间开始计划下一个60秒的定时器
        *  60秒后输出 out: collector ,重新开始计算
        * */
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
    }

    override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String),(String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
      state.value match {
        case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
          out.collect((key, count))
        case _ =>
      }
    }
  }

}
