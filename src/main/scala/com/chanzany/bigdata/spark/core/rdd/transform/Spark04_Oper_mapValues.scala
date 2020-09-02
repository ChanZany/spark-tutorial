package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_mapValues {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array(
      (1, "a"), (10, "b"),
      (11, "c"), (4, "d"),
      (20, "d"), (10, "e")))
    rdd.mapValues((_,1)).collect().foreach(println)
    //(1,(a,1))
    //(10,(b,1))
    //(11,(c,1))
    //(4,(d,1))

  }

}
