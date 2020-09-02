package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_coalesce {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //使用 coalesce 算子缩减RDD的分区数
    //可以简单的理解为合并分区(没有shuffle)
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16,4)
    println("原RDD分区数："+listRdd.partitions.size)
    val coalesceRDD: RDD[Int] = listRdd.coalesce(3)
    println("缩减后的RDD分区数："+coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output")
  }

}
