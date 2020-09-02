package com.chanzany.bigdata.spark.core.rdd.persistent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_rdd_persistent_checkpoint2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("persistentDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
//      println("map....")
      (num, 1)
    })

    //TODO 检查点操作会切断血缘关系。一旦数据丢失不会从头读取数据
    // 因为检查点会将数据保存到分布式存储系统中，数据相对而言比较安全，不容易丢失
    // 所以会切断血缘，等同于产生新的数据源
    sc.setCheckpointDir("ckpt")
    mapRDD.checkpoint()

    println(mapRDD.toDebugString)
    println(mapRDD.collect().mkString(","))
    println(mapRDD.toDebugString)




    sc.stop()
  }
}
