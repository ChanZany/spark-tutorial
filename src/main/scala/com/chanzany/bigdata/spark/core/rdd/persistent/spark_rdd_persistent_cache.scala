package com.chanzany.bigdata.spark.core.rdd.persistent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_rdd_persistent_cache {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("persistentDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map....")
      (num, 1)
    })

    // 将计算结果进行缓存
    // 默认的缓存是存储在Executor端的内存中，数据量大的时候，该如何处理
    // TODO 缓存cache()底层调用的是persist()
    //  persist()方法在持久化数据时会采用不同的存储级别对数据进行持久化操作
    //  cache()的默认存储级别就是将数据保存到内存中(MEMORY_ONLY)
    //  cache()存储的数据在内存中，如果内存不够用，executor可以将内存的数据进行整理丢弃
    //  如果由于executor端整理内存导致缓存的数据丢失，那么数据操作依旧要从头执行
    //  如果需要从头执行，必须遵循血缘关系，所以cache操作不能删除血缘关系
    val cacheRDD: mapRDD.type = mapRDD.cache()

    //TODO collect
    println(cacheRDD.collect().mkString(","))

    println("***************************************")
    //TODO save
    cacheRDD.saveAsTextFile("output")

    sc.stop()
  }
}
