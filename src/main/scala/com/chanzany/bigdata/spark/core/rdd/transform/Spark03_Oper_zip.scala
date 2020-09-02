package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper_zip {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // 使用zip算子合并两个RDD的所有元素为K-V
    // 限制条件：1. 两个RDD的分区数相同。2.两个RDD的元素个数相同
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "b", "c"), 3)

    //    val rdd2: RDD[String] = sc.parallelize(Array("a","b","c","d"),3) //Can only zip RDDs with same number of elements in each partition
    //    val rdd2: RDD[String] = sc.parallelize(Array("a", "b", "c"), 2) //Can't zip RDDs with unequal numbers of partitions: List(3, 2)

    val zipRdd: RDD[(Int, String)] = rdd1.zip(rdd2)
    zipRdd.collect().foreach(println)

  }

}
