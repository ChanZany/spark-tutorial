package com.chanzany.bigdata.spark.core.sharedVariables.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_bc_intro3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_bc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO Spark广播变量
    // 广播变量：分布式共享只读变量
    val list = List(("a", 4), ("b", 5), ("c", 6))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    //TODO 声明广播变量
    val bclist: Broadcast[List[(String, Int)]] = sc.broadcast(list)


    val resRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (word, count1) => {
        var count2 = 0
        // TODO 使用广播变量
        for (kv <- bclist.value) {
          if (word == kv._1) {
            count2 = kv._2
          }
        }

        (word, (count1, count2))
      }
    }

    println(resRDD.collect().mkString(","))

    sc.stop()
  }
}
