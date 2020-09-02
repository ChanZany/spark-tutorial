package com.chanzany.statisticProject.dao

import com.chanzany.statisticProject.bean.UserVisitAction
import com.chanzany.summerFrame.core.TDAO
import org.apache.spark.rdd.RDD

class PageFlowDao extends TDAO {
  def getUserVisitAction(path: String) = {
    val rdd: RDD[String] = readFile(path)
    rdd.map(
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong)
      }
    )
  }
}
