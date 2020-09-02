package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_takeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(8, 6, 2, 4, 0, 2, 1, 3))
    //TODO takeOrdered:返回RDD中按照指定规则排序后的前n个元素
    val res = rdd.takeOrdered(3)(Ordering[Int].reverse)
    println(res.mkString(","))
    sc.stop()

  }
}
