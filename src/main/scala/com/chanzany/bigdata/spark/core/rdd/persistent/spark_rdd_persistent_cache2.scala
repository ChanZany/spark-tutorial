package com.chanzany.bigdata.spark.core.rdd.persistent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_rdd_persistent_cache2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("persistentDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map....")
      (num, 1)
    })
    //TODO cache操作在行动算子执行后，会在血缘关系中增加和缓存相关的依赖
    // cache操作不会切断血缘，一旦发生错误，可以重新执行
    val cacheRDD: mapRDD.type = mapRDD.cache()
    println(cacheRDD.toDebugString)
    println(cacheRDD.collect().mkString(",")) //行动算子
    println(cacheRDD.toDebugString)




    sc.stop()
  }
}
