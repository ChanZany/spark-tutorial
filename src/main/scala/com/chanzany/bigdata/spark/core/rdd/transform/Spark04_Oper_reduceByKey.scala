package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_reduceByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc = new SparkContext(config)

    val words: RDD[String] = sc.parallelize(Array("hello", "world", "atguigu", "hello", "are", "go"))
    val wordPairsRdd: RDD[(String, Int)] = words.map(word => (word, 1))

    val resRdd: RDD[(String, Int)] = wordPairsRdd.reduceByKey(_ + _)
    resRdd.collect().foreach(println)
  }

}
