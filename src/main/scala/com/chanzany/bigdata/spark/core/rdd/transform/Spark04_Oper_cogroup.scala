package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_cogroup {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
    val sc = new SparkContext(config)
    //在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
    val rdd1 = sc.parallelize(Array((1, 10),(2, 20),(1, 100),(3, 30)),1)
    val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c")),1)
    rdd1.cogroup(rdd2).collect().foreach(println)

  }

}
