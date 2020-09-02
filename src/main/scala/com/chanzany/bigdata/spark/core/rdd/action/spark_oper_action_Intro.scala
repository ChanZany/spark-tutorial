package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_Intro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)

//    Spark 行动算子：不会产生新的RDD,而是触发作业的执行
//     行动算子执行后，会获取到作业的执行结果
//    Spark 转换算子：不会产生作业，只是功能逻辑的一层层包装拓展
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val data: Array[Int] = rdd.collect()
    data.foreach(println)


    sc.stop()

  }
}
