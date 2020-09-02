package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO reduce:简化规约
    val res: Int = rdd.reduce(_ + _)
    println(res) //10
    sc.stop()

  }
}
