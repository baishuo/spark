/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.mutable.ArrayBuffer
import java.util

/**
 * Created by baishuo on 12/8/14.
 */
object HiveStringTool {
  //ss的格式如group|by,    得到指定的单词或单词串，在字符串里的位置, 像group by这样的，要忽略中间的空格数量
  //xxx group by xxxx, 如果ss是group|by的话，返回（4， 11）
  def getIndex(s: String, ss: String): (Int, Int) = {
    val upperString = s.toUpperCase
    val upperArray = ss.split("\\|").map(_.toUpperCase)
    var canbreak = false
    var tempEnd = 0
    var index = -1
    for (i <- 0 until upperArray.size if !canbreak) {
      if (i == 0) {
       val tempIndex = upperString.indexOf(upperArray(i))
       if (tempIndex == -1) { // the first one can not be matched return -1 directly
         index = -1
         canbreak = true
       } else { // the first is matched
         index = tempIndex
         tempEnd = tempIndex + upperArray(i).length
       }
      } else {
        val tempIndex = upperString.indexOf(upperArray(i))
        /*
         *虽然第一个匹配了，但是后面的没有匹配，就返回一。这里有个潜在的bug
         * 例如 limit limit to 就不会匹配关键词limit to
         * */
        if (tempIndex == -1) {
          index = -1
          canbreak = true
        } else { // if matched
          // do not need to change index
          //if there is only " " between two words,continue
          if (upperString.substring(tempEnd,tempIndex).trim.length == 0){ // 如果中间隔的只是空格
            tempEnd = tempIndex + upperArray(i).length
          } else { // if there is not only " ",  return -1 directly
            index = -1
            canbreak = true
          }
        }
      }
    }
    assert(index < tempEnd, "begin master less than end!!!")
    (index, tempEnd)
  }

  def getIndex(s: String, ss: String, indexOfString: Int): (Int, Int) = {
    val upperString = s.toUpperCase
    val upperArray = ss.split("\\|").map(_.toUpperCase)
    var canbreak = false
    var tempEnd = 0
    var index = -1
    for (i <- 0 until upperArray.size if !canbreak) {
      if (i == 0) {
        val tempIndex = upperString.indexOf(upperArray(i), indexOfString)
        if (tempIndex == -1) { // the first one can not be matched return -1 directly
          index = -1
          canbreak = true
        } else { // the first is matched
          index = tempIndex
          tempEnd = tempIndex + upperArray(i).length
        }
      } else {
        val tempIndex = upperString.indexOf(upperArray(i), indexOfString)
        if (tempIndex == -1) { // if any one can not be matched return -1 directly
          index = -1
          canbreak = true
        } else { // if matched
          // do not need to change index
          //if there is only " " between two words,continue
          if (upperString.substring(tempEnd,tempIndex).trim.length == 0){
            tempEnd = tempIndex + upperArray(i).length
          } else { // if there is not only " ",  return -1 directly
            index = -1
            canbreak = true
          }
        }
      }
    }
    assert(index < tempEnd, "begin master less than end!!!")
    (index, tempEnd)
  }
  // 得到字符串s中,字符串dst的所有index
  def getAllIndex(s: String, dst: String, buffer: ArrayBuffer[(Int, Int)]) = {
    var indexOfString = 0
    var canbreak = false;
    while (!canbreak) {
      val index = getIndex(s, dst, indexOfString)
      if (index._1 != -1) {
        buffer += index
        indexOfString = index._2
      } else {
        canbreak = true
      }
    }
  }

  def getIndices(s: String, ss: Array[String]): Array[(Int, Int)]  = {
    // ss.map(str => getIndex(s, str))
    val list = ArrayBuffer[(Int, Int)]()
    var index = 0
    for (str <- ss) {
      getAllIndex(s, str, list)
    }
    list.toArray
  }
  // if more than one key words are matched, we need to choose the one has min start
  // 对于所有匹配的关键字，选取最左边的作为作为截取的标志
  def getMinIndex(indices: Array[(Int, Int)]): (Int, Int) = {
    def getMin(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
      if (a._1 < b._1) a else b
    }
    indices.foldLeft(indices(0))(getMin)
  }

  /*
   * 把字符串数组里的每一个元素，用"|" 连接在一起，成为一个新字符串
   * trim all string, and contract them by "|"
   * */
  def reConstructString(strings: Array[String]): String = {
    strings.foldLeft("")(_ + "|" + _).substring(1)
  }


  def handleRightbracke(str: String): (String, Boolean) = {
    var count = 0;
    var x: (String, Boolean) = (str,false)
    for (i <- 0 until str.length) {
      if (str.charAt(i) == '(') {
        count = count + 1
      } else if (str.charAt(i) == ')') {
        count = count - 1
      }
    }
    if (count == -1) {
      // 目前假设只可能丢掉最后的右括号
      assert(str.charAt(str.length -1) == ')', ") is not the last char!")
      if (str.charAt(str.length -1) == ')') {
        x = (str.substring(0, str.length - 1), true)
      }
    }
    x
  }

  /**
   * case class 用来表达 截取的字符串的相关信息
   * str 表示抽取出来的字符串
   * Strbegin 表示str之前的一段
   * 最后一个参数表示是否 移除最后一个右括号
   * */
  case class indexInfo(str: String,
                       Strbegin: String,
                       begin: Int,
                       end: Int,
                       removeRightBracke: Boolean)
  /*
     * 对外调用那个的最主要的函数
     * 返回抽取的介于start 和与子最近的end 之间的字符串,
     * start like :"group|by"   end like: "group by|limit|having|distribute to"
     */
  def getSubStr(sql: String, start: String, end: String) : Array[indexInfo] = {
    var tempSql = sql
    val strs = ArrayBuffer[indexInfo]()
    val ends = end.split("\\|")
    var canbreak = false
    var startTurple = getIndex(tempSql, start)
    println("tempSql:" + tempSql)
    var indexDelta = 0
    while (!canbreak) {
      if (startTurple._1 == -1) { // 没有匹配上start 退出
        canbreak = true
      } else {
        val arrays = end.split("\\|").map(_.split(" ")).map(reConstructString(_))
        val endIndicesTupleArray = getIndices(tempSql, arrays)
        // 至少有一个end被匹配上了
        if (endIndicesTupleArray.exists(x => (x._1 > -1) && (x._1 > startTurple._1))) {
        val minEndIndicex = getMinIndex(endIndicesTupleArray.filter( x => (x._1 > -1) && (x._1 > startTurple._1)) ) // 得到离start最近的一个
          println("---" + startTurple, minEndIndicex)
          val startAndEnd = (indexDelta + startTurple._2, indexDelta + minEndIndicex._1) // 记录抽出的字符串在整个打字符串中的位置
          val strAndRightBracke: (String, Boolean) =  handleRightbracke(tempSql.substring(startTurple._2, minEndIndicex._1).trim) // handleRightbracke(tempSql.substring(startTurple._2, minEndIndicex._1).trim)
          val beforeStr = tempSql.substring(0,startTurple._2)
          val indInfo = indexInfo(strAndRightBracke._1, beforeStr, startAndEnd._1, startAndEnd._2, strAndRightBracke._2)
          strs += indInfo
          indexDelta = indexDelta + minEndIndicex._1
          tempSql = tempSql.substring(minEndIndicex._1) // 把后面的字符串，赋值给tempSql
          println ("tempSql:" + tempSql)
        } else { // 匹配start，没匹配end，把start后面的都放到Array里面
          val startAndEnd = (indexDelta + startTurple._2, indexDelta + tempSql.length)
          val strAndRightBracke: (String, Boolean) = handleRightbracke(tempSql.substring(startTurple._2, tempSql.length).trim) // handleRightbracke(tempSql.substring(startTurple._2, tempSql.length).trim)
          val beforeStr = tempSql.substring(0,startTurple._2)
          val indInfo = indexInfo(strAndRightBracke._1, beforeStr, startAndEnd._1, startAndEnd._2, strAndRightBracke._2)
          strs += indInfo
          canbreak = true
        }
      }
      startTurple = getIndex(tempSql, start) // 下一次循环的条件
    }
    assert(strs.size > 0, "strs is empty!")
    strs.toArray
  }

  def getFinalString(aaa: Array[HiveStringTool.indexInfo]): String = {
    aaa.foreach(str => println("str---:" + str + "|" + str.str))
    val strs = aaa.map(info => new GrpParser(info.str).parser)
    val strBuffer = new StringBuffer
    for (i <- 0 until aaa.length) {
      var str = strs(i)
      if (aaa(i).removeRightBracke) {
        str = str + ")"
      }
      strBuffer.append(aaa(i).Strbegin).append(" ").append(str).append(" ")
    }
    println("result:" + strBuffer.toString)
    strBuffer.toString
  }

  def main(args: Array[String]) {
    println("wakaka")
    var sql = "aaaa bbbbb group by ccc limit of ssss"
    var end = ""
    val sql1 = "aaaa bbbbb " +
               "group by ccc ddd " +
               "group by eee fff" +
               " limit of Group  by ssss distribute   to " +
               "ggg group by hhh"
    val sql2 = """select
                             nvl(S2.COUNTY_ID,'999')                         CITY_ID
                             ,nvl(S1.BUSI_ID,-9)                             BUSI_ID
                             ,nvl(S2.BRAND_ID,'-1')                          BRAND_ID
                             ,count( case when S1.ACCESS_CNT > 0 then S1.user_id else null end) USER_NUM
                             ,sum(S1.FLOW)                                   USER_FLOW
                             ,sum(S1.ACCESS_CNT)                             USER_ACCE_CNT
                             ,sum(S1.FLOW)*1.0000/sum(S1.ACCESS_CNT)         AVG_ACCE_FLOW
                            ,case when sum(S1.ACCESS_CNT) = 0 then 0 else sum(S1.ACCESS_CNT)*1.0000/count( case when S1.ACCESS_CNT > 0 then S1.user_id else null end) end                  AVG_ACCE_CNT
                             ,sum(S2.ARPM*S1.FLOW*1.0000/ 1048576)           BUSI_TOTAL_FEE
                             ,case when count( case when S1.ACCESS_CNT > 0 then S1.user_id else null end) = 0 then 0 else sum(S2.ARPM*(S1.FLOW*1.0000/ 1048576))/count( case when S1.ACCESS_CNT > 0 then S1.user_id else null end) end            ARPU
                             ,case when sum(S1.FLOW) = 0 then 0 else sum(S2.ARPM*S1.FLOW)/sum(S1.FLOW) end       ARPM
                             ,count( case when BUSI_STABLE_FLAG = '1' then S1.user_id else null end)             STABLE_USER_NUM
                             ,count( case when BUSI_ACTIVITY_FLAG = '1' then S1.user_id else null end)           ACTIVITY_USER_NUM
                             ,count( case when BUSI_SILENCE_FLAG = '1' and S1.ACCESS_CNT > 0 then S1.user_id else null end)  SILENCE_USER_NUM
                         from DW_CAL_BUSI_USER_D_YYYYMMDD  S1
                             inner join  DW_USER_ALL_INFO_YYYYMM  S2
                             on (S1.user_id = S2.user_id  and S1.ACCESS_CNT > 0)
                         group by cube(S2.COUNTY_ID ,S2.BRAND_ID),S1.BUSI_ID"""
    val sql3 =
      """
        select tc1 from (select tc1 from t1 group by cube( tc1) , cube(),() left     join select tc1 from t2 group by rollup(tc1)) group by  rollup(tc1), cube(tc1), tc1
      """.stripMargin
    val sql4 =
      """
        select tc1 from (select tc1 from t1 group by cube(tc1)  left     join select tc1 from t2 group by rollup(tc1)) group by  rollup(tc1), cube(tc1), tc1
      """.stripMargin
    val sql5 =
      """
        select tc1 from t1 group by  rollup(tc1), cube(tc1), tc1
      """.stripMargin
    val end1 = "group by|limit|having|distribute to"
    val end2 = "grouP by|Limit of|having|distriBute to|left join"
    val end3 = "grouP by|Limit of|having|distriBute to|left join"
    val arr = "gRoup|by"
    val arr2 = "gRoup|by1"
    //println("match gRoup by" + getIndex(sql, arr))
    //println("match gRoup by" + getIndex(sql, arr2))

    // getSubStr(sql, "group|by", "group by|limit|having|distribute to").foreach( println (_))
    println("------------------------------------------------")
    // getSubStr(sql1, "group|by", end2).foreach( println (_))
    // getSubStr(sql2, "group|by", end2).foreach(str => println ("str:" + str))
    // getSubStr(sql3, "group|by", end2).foreach(str => println ("str:" + str))

    // val aaa = getSubStr(sql2, "group|by", end2).map(new GrpParser(_).parser).foreach(str => println ("str---:" + str))
     sql = sql5
     end = end2
     val aaa = getSubStr(sql, "group|by", end)
     getFinalString(aaa)
  }

}
