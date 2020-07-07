package com.bbd.FlinkTest.imooc

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

object DatasetDataSource {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    fromCollection(env)
//    textFile(env)
//    csvFile(env)
//    readRecursionFile(env)
    readCompressionFile(env)
  }


  def fromCollection(env: ExecutionEnvironment) :Unit = {
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  def textFile(env : ExecutionEnvironment):Unit = {
//    val filepath = "E:\\idea_workspace\\FlinkTest\\data\\vertex_person.csv"
    val filepath = "E:\\idea_workspace\\FlinkTest\\data"
    //可以读文件夹也可以读具体的文件
    env.readTextFile(filepath).print()
  }

  def csvFile(env : ExecutionEnvironment):Unit = {
    //    val filepath = "E:\\idea_workspace\\FlinkTest\\data\\vertex_person.csv"
    case class person(id: Int,zjhm:String,name:String,country:String,address:String)
    val filepath = "E:\\idea_workspace\\FlinkTest\\data\\vertex_person.csv"
//    env.readCsvFile[(Int,String,String,String,String)](filepath,ignoreFirstLine = true).print()
//    env.readCsvFile[(String,String,String)](filepath,ignoreFirstLine = true,includedFields = Array(1,2,3)).print()
//    env.readCsvFile[person](filepath,ignoreFirstLine = true).print()
    env.readCsvFile[jperson](filepath,ignoreFirstLine = true,pojoFields = Array("id","zjhm","name","country","address")).print()
  }

  def readRecursionFile(env : ExecutionEnvironment):Unit = {
    //读取递归文件夹
    val filepath = "E:\\idea_workspace\\FlinkTest\\data"
    var parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
//    env.readTextFile(filepath).withParameters(parameters).print()
    print(env.readTextFile(filepath).withParameters(parameters).count())
  }

  def readCompressionFile(env : ExecutionEnvironment):Unit = {
    //读取递归文件夹 支持这几种压缩格式
//    Compression method	File extensions	Parallelizable
//    DEFLATE	.deflate	no
//      GZip	.gz, .gzip	no
//      Bzip2	.bz2	no
//      XZ	.xz	no
    val filepath = "E:\\idea_workspace\\FlinkTest\\data\\compression"
    env.readTextFile(filepath).print()
  }

}
