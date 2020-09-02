package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 十种方式实现wordCount
 */
object Spark05_summary_wc10 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(List("a", "a", "a", "b", "b"))
    val mapRDD: RDD[Map[String, Int]] = rdd.map(word => Map[String, Int]((word, 1)))
    val tupRDD = rdd.map((_,1))
    //TODO 1.groupBy
    val groupRDD= tupRDD.groupBy(word => word._1)
    val res: RDD[(String, Int)] = groupRDD.map(elem => {
      (elem._1, elem._2.size)
    })
    println(res.collect().mkString(","))

    //TODO 2.groupByKey
    //TODO 3.reduceByKey
    //TODO 4.aggregateByKey
    //TODO 5.foldByKey
    //TODO 6.combineByKey
    //TODO 7.countByKey
    //TODO 8.countByValue


    //TODO 9.reduce:(Map,Map)=>Map

    val reduceRDD: Map[String, Int] = mapRDD.reduce(
      (map1, map2) => {
        map1.foldLeft(map2)(
          (map, kv) => {
            map.updated(kv._1, map.getOrElse(kv._1, 0) + kv._2)
          }
        )
      }
    )
    println(reduceRDD)
    //TODO 10.fold
    val foldRDD: Map[String, Int] = mapRDD.fold(Map[String, Int]())(
      (map1, map2) => {
        map1.foldLeft(map2)(
          (map, kv) => {
            map.updated(kv._1, map.getOrElse(kv._1, 0) + kv._2)
          }
        )
      }
    )
    println(foldRDD)
    //TODO 11.aggregate
    val aggreRDD: Map[String, Int] = rdd.aggregate(Map[String, Int]())(
      //分区内map元素累加
      (map, k) => {
        map.updated(k, map.getOrElse(k, 0) + 1)
      },
      //分区间map合并
      (map1, map2) => {
        map1.foldLeft(map2)(
          (map, kv) => {
            map.updated(kv._1, map.getOrElse(kv._1, 0) + kv._2)
          }
        )
      }
    )
    println(aggreRDD)

    sc.stop()

  }

}
