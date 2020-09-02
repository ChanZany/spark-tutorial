package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_mapPartition {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 2, 2),2)
    val res: RDD[(Int, Int)] = rdd.mapPartitions(
      iter => {
        var mapIter: Iterator[(Int, Int)] = null
        if (iter.hasNext){
           mapIter= List((iter.next(), iter.length)).toIterator
        }
        mapIter
      }
    )
    println(res.collect().mkString(","))
    //(1,1),(2,1)与想法不符
    // 原因在于迭代器的方法都会使得该迭代器指针下移

    sc.stop()



  }

}
