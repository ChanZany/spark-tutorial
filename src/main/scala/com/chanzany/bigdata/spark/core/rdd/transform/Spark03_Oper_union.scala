package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_union {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // 使用union算子合并两个RDD的所有元素，与传统并集有差别(不会去重)
    val rdd1: RDD[Int] = sc.makeRDD(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(5 to 10)
    val unionRdd: RDD[Int] = rdd1.union(rdd2)
    unionRdd.collect().foreach(println)

  }

}
