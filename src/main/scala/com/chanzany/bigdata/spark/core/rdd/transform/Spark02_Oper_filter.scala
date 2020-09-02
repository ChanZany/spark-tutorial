package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_filter {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //生成数据，按照指定规则进行数据过滤 
    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val filterRdd: RDD[Int] = listRdd.filter(i => i % 2 == 0)
    filterRdd.foreach(println)


  }

}
