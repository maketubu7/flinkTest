package com.bbd.FlinkTest.flinkSource
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object DatasetTranformation {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapFunction(env)
//    filterFunction(env)
//    mapPartitionFunction(env)
//    firstFunction(env)
//    flatmapFunction(env)
    joinFunction(env)
  }

  def mapFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    env.fromCollection(data).map((x:Int) => x + 1).print()
    env.fromCollection(data).map(_+1).print()
    env.fromCollection(data).map(x =>x+1).print()
    env.fromCollection(data).map((x) =>x+1).print()
  }

  def filterFunction(env: ExecutionEnvironment): Unit ={
    val data = 1 to 10
    env.fromCollection(data).map(_+1).filter(_ > 3).print()
  }

  def mapPartitionFunction(env: ExecutionEnvironment): Unit ={
    val stdutents = new ListBuffer[String]
    for (i <- 1 to 100){
      stdutents.append("stdu: "+ i.toString)
    }

    env.fromCollection(stdutents).mapPartition(v => v map {(_,1)}).print()
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    env.fromCollection(stdutents).mapPartition(v => v.map(x => (x,1))).print()
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    env.fromCollection(stdutents).map(x => x +"1").print()

  }

  def firstFunction(env: ExecutionEnvironment): Unit ={
    val stdutents = new ListBuffer[(Int,String)]
    stdutents.append((1,"a"))
    stdutents.append((1,"b"))
    stdutents.append((1,"c"))
    stdutents.append((2,"d"))
    stdutents.append((2,"e"))
    stdutents.append((2,"f"))
    stdutents.append((3,"g"))
    stdutents.append((3,"h"))
    stdutents.append((3,"i"))

    env.fromCollection(stdutents).first(2).print()
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    //分组取前几
    env.fromCollection(stdutents).groupBy(0).first(2).print()
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    //分组降序取前几
    env.fromCollection(stdutents).groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()

  }

  def flatmapFunction(env: ExecutionEnvironment): Unit ={
    val words = new ListBuffer[String]
    words.append("hadoop,flink")
    words.append("spark,flink")
    words.append("flink,flink")

    env.fromCollection(words).flatMap(_.split(",")).distinct.map((_,1)).groupBy(0).sum(1).print()
  }

  def joinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int,String)]
    info1.append((1,"a"))
    info1.append((2,"d"))
    info1.append((3,"g"))
    info1.append((4,"h"))

    val info2 = new ListBuffer[(Int,String)]
    info2.append((1,"a"))
    info2.append((2,"d"))
    info2.append((3,"g"))
    info2.append((5,"i"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    // where leftkey equalTo rightkey
    data1.join(data2).where(0).equalTo(0)
          .apply((first,second) => (first._1,first._2,second._2)).print()
    println("~~~~~~~~~~~~~~~~~~~~~~")
    data1.leftOuterJoin(data2).where(0).equalTo(0)
      .apply((first,second) => {
        if (second == null){
          (first._1,first._2,"-")
        }
        else {
          (first._1,first._2,second._2)
        }
      }).print()
  }



}
