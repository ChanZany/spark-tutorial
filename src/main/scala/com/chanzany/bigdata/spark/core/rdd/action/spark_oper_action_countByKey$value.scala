package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_countByKey$value {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(
      List(
        ("one",1), ("two",1), ("three",1), ("four",1),
        ("one",1), ("two",2), ("three",3),("four",4))
    )
    rdd.foreach(println)
    //TODO count:计算RDD的元素数量
    val res: Long = rdd.count()
    //TODO countByKey:计算RDD元素(Tuple2)中的各个Key对应的元素个数
    val res2  = rdd.countByKey()
    //TODO countByValue:计算RDD元素(Tuple2)中的各个(Key,value)对应的元素个数,也就是统计元素的出现次数
    val res3 = rdd.countByValue()
    println("*************************************")
    println(res)
    println("*************************************")
    res2.foreach(println)
    println("*************************************")
    res3.foreach(println)
    sc.stop()

  }
}
