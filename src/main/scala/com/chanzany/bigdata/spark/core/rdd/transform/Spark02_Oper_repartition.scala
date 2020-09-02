package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_repartition {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //使用repartition算子，将RDD各个分区的数据进行shuffle并重新分配到新的分区中(指定数目)
    val rdd: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println()
    rdd.collect().foreach(x => print(x+"\t"))
    println()
    val repartitionRdd: RDD[Int] = rdd.repartition(2)
    repartitionRdd.collect().foreach(x => print(x+"\t"))
    val arrs: RDD[Array[Int]] = repartitionRdd.glom()
    println()
    println(arrs.collect().size)
  }

}
