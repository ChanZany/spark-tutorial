package com.chanzany.bigdata.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 依赖关系
 * 1) RDD 血缘关系
 */
object spark_rddDep_intro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_dependency").setMaster("local")
    val sc = new SparkContext(conf)

    //TODO  new ParallelCollectionRDD
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    println(rdd.toDebugString)
    println("------------------------")

    //TODO new ParallelCollectionRDD -> new MapPartitionsRDD
    val wordRDD: RDD[String] = rdd.flatMap(
      string => {
        string.split(" ")
      }
    )
    println(wordRDD.toDebugString)
    println("------------------------")


    //TODO new MapPartitionsRDD -> new MapPartitionsRDD
    val wordPairRDD: RDD[(String, Int)] = wordRDD.map(
      word => (word, 1)
    )
    println(wordPairRDD.toDebugString)
    println("------------------------")

    //TODO new MapPartitionsRDD -> new ShuffledRDD
    //如果Spark计算过程中某一个节点计算失败，那么框架会尝试重新计算(在其他节点上)
    // spark想要重新计算就得直到数据的来源。并且还要直到数据经历了哪些计算
    // RDD不保存计算的数据，但是会保存元数据信息
    val reduceRDD: RDD[(String, Int)] = wordPairRDD.reduceByKey(_ + _)
    println(reduceRDD.toDebugString)
    println("------------------------")


//    println(reduceRDD.collect().mkString(","))

    sc.stop()
  }

}
