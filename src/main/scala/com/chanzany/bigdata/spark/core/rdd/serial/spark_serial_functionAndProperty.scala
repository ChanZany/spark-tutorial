package com.chanzany.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_serial_functionAndProperty {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("serial").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List(
      "hello world", "hello spark", "hello scala",
      "hadoop", "flink", "hbase", "spark", "zookeeper"
    ))

    val search = new Searcher("hello")
//    search.getMatchedRDD1(rdd).collect().foreach(println)
    search.getMatchedRDD2(rdd).collect().foreach(println)

    sc.stop()

  }
  //需求: 在 RDD 中查找出来包含 query 子字符串的元素

  // query 为需要查找的子字符串
  class Searcher(val query: String){
//  class Searcher(val query: String) extends Serializable {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s : String) ={
      s.contains(query)
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) ={
      rdd.filter(isMatch)  //isMatch方法属于this对象
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) ={
//      rdd.filter(_.contains(query)) //query属性属于this对象
      val s:String = query
      rdd.filter(_.contains(s)) //s(String本身可以序列化)不属于this对象，所以不用当前类可序列化也可以成功运行
    }
  }

}
