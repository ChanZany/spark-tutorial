package com.chanzany.bigdata.spark.core.rdd.persistent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_rdd_persistent_checkpoint {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("persistentDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map....")
      (num, 1)
    })
    //TODO 将比较耗时，比较重要的数据一般保存至分布式文件系统中
    // 使用checkpoint方法将数据保存在文件中(写入磁盘)
    // 执行checkpoint方法前应该设定(sc中)检查点的保存目录
    // 检查点操作中为了保证数据的准确性，在执行时会启动新的Job(重新执行一遍)
    // 为了提高性能，检查点操作常常与cache联合使用
    sc.setCheckpointDir("ckpt")
    mapRDD.cache()
    mapRDD.checkpoint()

    println(mapRDD.collect().mkString(","))
    println("*********************************")
    println(mapRDD.collect().mkString("&"))




    sc.stop()
  }
}
