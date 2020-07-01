package com.bbd.FlinkTest.dao

/**
  * @Author: maketubu
  * @Date: 2019/12/9 20:08
  */
object caseClass {
  case class ServerMsg(serverId: String, isOnline: Boolean, timestamp: Long)
  case class Person(jid: String, sfzh: String, xm: String, dz: String)
}
