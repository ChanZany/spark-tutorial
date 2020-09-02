package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_collect {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO collect:采集数据
    // collect方法会把所有分区计算的结果拉取到当前控制台所在节点内存中，可能会出现内存溢出
    val res: Array[Int] = rdd.collect()
    println(res)
    sc.stop()

  }
}
