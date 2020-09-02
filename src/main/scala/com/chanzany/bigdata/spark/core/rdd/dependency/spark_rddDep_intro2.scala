package com.chanzany.bigdata.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 依赖关系
 * 2) RDD 依赖关系
 */
object spark_rddDep_intro2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_dependency").setMaster("local")
    val sc = new SparkContext(conf)

    //TODO OneToOneDependency@7fcff1b9
    // OneToOneDependency：依赖关系中，当前RDD的数据分区和其依赖的RDD的数据分区一一对应
    // 属于窄依赖(NarrowDependency)
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    println(rdd.dependencies)
    println("------------------------")

    //TODO OneToOneDependency@5f8890c2
    val wordRDD: RDD[String] = rdd.flatMap(
      string => {
        string.split(" ")
      }
    )
    println(wordRDD.dependencies)
    println("------------------------")


    //TODO ShuffleDependency@5e9456ae
    // ShuffleDependency(N to N):依赖关系中，当前RDD的数据分区和其依赖的RDD的数据分区经过shuffle操作后
    // 不再一一对应。
    // 属于宽依赖(WideDependency)
    val wordPairRDD: RDD[(String, Int)] = wordRDD.map(
      word => (word, 1)
    )
    println(wordPairRDD.dependencies)
    println("------------------------")


    val reduceRDD: RDD[(String, Int)] = wordPairRDD.reduceByKey(_ + _)
    println(reduceRDD.dependencies)
    println("------------------------")


//    println(reduceRDD.collect().mkString(","))

    sc.stop()
  }

}
