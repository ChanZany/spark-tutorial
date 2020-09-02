package com.chanzany.summerFrame.core

import com.chanzany.summerFrame.util.EnvUtils
import org.apache.spark.rdd.RDD

/**
 * 数据访问对象(数据交互层)，主要负责和数据库如mysql之间的交互
 */
trait TDAO {
  def readFile(path:String) ={
    val fileRDD: RDD[String] = EnvUtils.getEnv().textFile(path)
    fileRDD
  }
}
