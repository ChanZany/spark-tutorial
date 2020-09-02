package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark04_Oper_partitionBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(config)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    println(rdd.partitions.size)
    //对rdd重新分区
    val resRdd: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    val arrs: Array[Array[(Int, String)]] = resRdd.glom().collect()
    for (arr <- arrs) {
      println()
      arr.foreach(x => print(x+"\t"))
      println()
    }

  }

}
