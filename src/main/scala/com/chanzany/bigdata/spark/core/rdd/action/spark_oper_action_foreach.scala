package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_foreach {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO foreach:方法(Driver端执行)
    // 集合的方法中的代码是在当前节点(Driver)中执行的
    // foreach方法是在当前节点内存中完成数据的循环
    rdd.collect().foreach(println)
    println("******************************************")
    //TODO foreach:算子(Executor端执行)
    // 算子的逻辑代码是在分布式计算节点Executor处执行的
    // foreach算子可以将循环在不同计算节点(Executor)完成
    rdd.foreach(println)
    sc.stop()

  }
}
