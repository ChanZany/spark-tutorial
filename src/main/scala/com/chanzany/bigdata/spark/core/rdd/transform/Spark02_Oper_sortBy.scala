package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_sortBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)


    //使用 sortBy 算子 对RDD内的数据进行排序操作
    val rdd: RDD[Int] = sc.parallelize(List(3, 2, 1, 5))
    val sortRdd: RDD[Int] = rdd.sortBy(i => i,false) //ascending默认为true表示升序
    sortRdd.collect().foreach(println)

  }

}
