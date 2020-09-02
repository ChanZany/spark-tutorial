package com.chanzany.bigdata.spark.core.sharedVariables.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_bc_intro2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_bc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO Spark广播变量
    // join操作会有笛卡儿积效果，计算量会急剧增多。如果有Shuffle操作，那么性能会非常低

    //TODO 为了解决join出现性能问题，可以将数据独立出来，防止shuffle操作
    // 这样的话会导致数据会绑定复制给每一个task，那么executor内存中有大量冗余数据，性能也会受到影响
    // 所以可以采用广播变量，将数据之间保存到executor的一块共享内存中，所有task共享该广播变量
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list = List(("a", 4), ("b", 5), ("c", 6))

    // (a,(1,4)),(b,(2,5)),(c,(3,6))
    // (a,1),(a,4)=>(a,(1,4))
    val resRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (word, count1) => {
        var count2 = 0
        for (kv <- list) {
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
