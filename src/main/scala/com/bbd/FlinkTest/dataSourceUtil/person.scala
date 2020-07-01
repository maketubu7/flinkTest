package com.bbd.FlinkTest.dataSourceUtil

import java.text.SimpleDateFormat
import java.util.Calendar

import com.bbd.FlinkTest.dataSource.address

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @Author: maketubu
  * @Date: 2020/2/28 12:58
  */
object person {
  private val random = new Random()

  def get_sfzh() : String = {
    val xzqhs = address.xzqh.keys.toBuffer
    val tmp_years = (1990 to 2020).map(x => x.toString)
    val years = ArrayBuffer[String]()
    for (e <- tmp_years) years += e
    val xzqh = xzqhs(random.nextInt(xzqhs.length))
    val checkid = ArrayBuffer[String]("223","332","647")
    val sex = ArrayBuffer[String]("1","2","X")
    val csrq = xzqh + getCsrq()
    val presfzh = csrq.concat(random.nextInt(9).toString).concat(random.nextInt(9).toString).concat(random.nextInt(9).toString)
    val sfzh = presfzh + check_id(presfzh)
    sfzh
  }

  private def check_id(presfz:String): String = {
    val pre = presfz.toList.map(x => x.toInt)
    val wi = List(7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2)
    val checkCode = List('1','0','X','9','8','7','6','5','4','3','2')
    val res = pre.zip(wi)
    var sum = 0
    for (e <- res){
      sum += e._1 * e._2
    }
    val checkid = checkCode(sum % 11)
    checkid.toString
  }

  private def getCsrq(start:String="1990-01-01",end:String="2020-01-01") :String = {
    val myformat = new SimpleDateFormat("yyyy-MM-dd")
    val start_date = myformat.parse(start)
    val end_date = myformat.parse(end)
    val days = ((end_date.getTime - start_date.getTime) /(1000*60*60*24)).toInt
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.DAY_OF_YEAR,random.nextInt(days))
    val birthday = cal.getTime
    new SimpleDateFormat("yyyyMMdd").format(birthday)
  }

}
