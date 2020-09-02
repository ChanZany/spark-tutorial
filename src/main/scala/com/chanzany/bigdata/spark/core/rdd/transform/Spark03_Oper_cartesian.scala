package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_cartesian {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // 使用intersection算子求两个RDD的笛卡儿积(慎用！！)
    val rdd1: RDD[Int] = sc.makeRDD(1 to 3)
    val rdd2: RDD[Int] = sc.parallelize(2 to 5)
    val cartesianRdd: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    cartesianRdd.collect().foreach(println)

  }

}
