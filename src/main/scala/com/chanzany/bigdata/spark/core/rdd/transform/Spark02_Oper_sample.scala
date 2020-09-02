package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_sample {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //生成数据，按照指定规则进行数据采样
    //参数1：是否放回
    //参数2：抽取的概率阈值，掷到的随机数如果小于该阈值就抽取
    //参数3：随机数种子
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
//    val sampleRdd: RDD[Int] = listRdd.sample(false, 0.4, 1)
    val sampleRdd: RDD[Int] = listRdd.sample(true, 0.6, 10)
    sampleRdd.collect().foreach(println)


  }

}
