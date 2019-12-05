package com.bbd.FlinkTest.flinkSource

import java.lang

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector



/**
  * @Author: maketubu
  * @Date: 2019/12/3 17:47
  */
object simpleStreamCommonSource {
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

    /**stream
    基本算子 keyby 根据某个key进行分区， 按照某一个字段进行求和*/
    //      stream.map(r => getRecord(r)).keyBy(r => r.sexy).sum("shoppingTime").map{r => (r.sexy,r.shoppingTime)}.print("s: ")
    /**reduce*/
    //       stream.map(r => getRecord(r)).keyBy(r => r.sexy).reduce((t1: UserRecord, t2: UserRecord) =>{
    //         UserRecord(t1.name+ "-"+ t2.name, t1.sexy, t1.shoppingTime+t2.shoppingTime)
    //       }).print()
    /**window 根据一个窗口的时间对数据进行操作 有三种时间Event Process Ingestion 需要对env进行设置*/
    //    stream.map(r => getRecord(r)).keyBy(r => r.sexy).timeWindow(Time.seconds(1)).sum("shoppingTime").print()

    /**split 根据自定义规则将DataStream 分为很多splitDataStream, 在通过select方法进行选择*/
    //    val split: SplitStream[UserRecord] = stream.map(r => getRecord(r)).split(p =>{
    //      p.sexy match {
    ////          Seq 也行
    //        case "man" => List("man")
    //        case "woman" => List("woman")
    //        case _ => List("other")
    //      }
    //    })

    //
    //    val woman: DataStream[UserRecord] = split.select("woman")
    //    val man = split.select("man")
    //    val other = split.select("other")

    //connect 将两个流合并为一个流

    val peopleStream = stream.map(r => getPeople(r))
    val userStream = stream.map(r => getRecord(r))

    val connect: ConnectedStreams[People, UserRecord] = peopleStream.connect(userStream)

    peopleStream.map(r => (r.name, r.sexy)).keyBy(0).process(new CountWithTimeoutFunction()).print()

    // groupby sum计数，对应spark的reduceBykey
    //    set.map(r => getRecord(r)).map{r => (r.sexy,1)}.groupBy(0).sum(1).print()

    //
    //    other.print()
    //    man.print()
    //    val outputTag = OutputTag[String]("side-output")
    //
    //    val process = stream.map(r => getRecord(r)).process(new ProcessFunction[String] {
    //      override def processElement(value: String, ctx: ProcessFunction[String], out: Collector[String]): Unit = {
    //        // 将数据发送到常规输出中
    //        out.collect(value)
    //        // 将数据发送到侧输出中
    //        ctx.
    //      }
    //    })
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
      val current: CountWithTimestamp = state.value match {
        case null =>
          CountWithTimestamp(value._1, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }

      // 回写state状态
      state.update(current)

      /** 从当前事件时间开始计划下一个60秒的定时器 */
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
