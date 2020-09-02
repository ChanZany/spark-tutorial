package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_first {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO first:返回RDD中的第一个元素
    val res = rdd.first()
    println(res) //1
    sc.stop()

  }
}
