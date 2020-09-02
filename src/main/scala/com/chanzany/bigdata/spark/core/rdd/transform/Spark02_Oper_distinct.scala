package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_distinct {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //使用distinct算子对数据去重，可以设置numPartitions指定去重后数据存放的分区数量
    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))
//    val distinctRDD: RDD[Int] = listRdd.distinct()
    val distinctRDD: RDD[Int] = listRdd.distinct(numPartitions = 2)

    distinctRDD.saveAsTextFile("output")


  }

}
