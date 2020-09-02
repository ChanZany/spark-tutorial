package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_sortByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array(
      (1, "a"), (10, "b"),
      (11, "c"), (4, "d"),
      (20, "d"), (10, "e")))
    rdd.sortByKey().collect().foreach(println) //默认ascending = True 正序
    rdd.sortByKey(false).collect().foreach(println) //false 逆序


  }

}
