package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_subtract {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // 使用subtract算子求RDD的差集
    val rdd1: RDD[Int] = sc.makeRDD(3 to 8)
    val rdd2: RDD[Int] = sc.parallelize(1 to 5)
    val subRdd: RDD[Int] = rdd1.subtract(rdd2)
    subRdd.collect().foreach(println)

  }

}
