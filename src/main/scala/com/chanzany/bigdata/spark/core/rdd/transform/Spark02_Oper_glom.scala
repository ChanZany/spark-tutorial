package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_glom {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //glom:将每一个分区的元素合并成一个数组，形成新的 RDD 类型是RDD[Array[T]]
    val listRdd: RDD[Int] = sc.makeRDD(1 to 18, 4)
    val glomRdd: RDD[Array[Int]] = listRdd.glom()
    glomRdd.collect().foreach(arr => {
      println(arr)
      arr.foreach(x => print(x + "\t"))
      println()
    })


  }

}
