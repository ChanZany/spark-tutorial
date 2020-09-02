package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_aggregate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    //TODO aggregate:聚合,参数和含义与aggregateByKey一致
    // * (初始值)(分区内运算,分区间运算)
    // 区别：
    // * aggregateByKey:初始值只参与到分区内计算
    // * aggregate:初始值同时参与分区内和分区间计算
    val res = rdd.sum()
    //    val res2 = rdd.aggregate(0)(math.max(_, _), _ + _)
//    val res2 = rdd.aggregate(5)((x, y) => math.max(x, y), (p1, p2) => p1 + p2)
    val res2 = rdd.aggregate(10)(_+_,_+_)
    println(res)
    println(res2) //66
    sc.stop()

  }
}
