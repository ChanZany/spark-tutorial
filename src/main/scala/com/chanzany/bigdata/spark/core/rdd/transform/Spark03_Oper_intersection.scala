package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_intersection {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // 使用intersection算子求两个RDD的交集
    val rdd1: RDD[Int] = sc.makeRDD(1 to 7)
    val rdd2: RDD[Int] = sc.parallelize(5 to 10)
    val intersectionRdd: RDD[Int] = rdd1.intersection(rdd2)
    intersectionRdd.collect().foreach(println)

  }

}
