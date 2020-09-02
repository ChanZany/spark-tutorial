package com.chanzany.bigdata.spark.core.sharedVariables.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_bc_intro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_bc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO Spark广播变量
    // join操作会有笛卡儿积效果，计算量会急剧增多。如果有Shuffle操作，那么性能会非常低

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    // (a,(1,1)),(b,(2,2)),(c,(3,3))
    println(joinRDD.collect().mkString(","))

    sc.stop()
  }
}
