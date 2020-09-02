package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark04_Oper_groupByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(config)

    val words: RDD[String] = sc.parallelize(Array("hello", "world", "atguigu", "hello", "are", "go"))
    val wordPairsRdd: RDD[(String, Int)] = words.map(word => (word, 1))
    val group: RDD[(String, Iterable[Int])] = wordPairsRdd.groupByKey()

//    for (g<-group){
//      println(g._1,g._2.sum)
//    }
    val resRdd: RDD[(String, Int)] = group.map(g => Tuple2(g._1, g._2.sum))
    resRdd.foreach(println)
  }

}
