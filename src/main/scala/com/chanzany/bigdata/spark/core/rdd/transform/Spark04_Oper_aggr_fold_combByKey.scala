package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey其实是aggregateByKey的简化操作，seqop和combop相同
 */
object Spark04_Oper_aggr_fold_combByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foldByKey")
    val sc = new SparkContext(config)
    //使用foldByKey算子，计算相同key对应值的相对结果
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(
      ("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)))
    aggreSum(rdd)
    foldSum(rdd)
    combineSum(rdd)

  }

  def aggreSum(rdd: RDD[(String, Int)]): Unit = {
    println("aggregateByKey get same key sum value")
    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
  }

  def foldSum(rdd: RDD[(String, Int)]): Unit = {
    println("foldByKey get same key sum value")
    rdd.foldByKey(0)(_ + _).collect().foreach(println)
  }

  def combineSum(rdd: RDD[(String, Int)]): Unit = {
    println("combineByKey get same key sum value")
    //rdd.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y).collect().foreach(println)
    rdd.combineByKey(x => x, (x: Int, y) => x + y, (x: Int, y:Int) => x + y).collect().foreach(println)
    // 报错missing parameters type
    //rdd.combineByKey(x => x, (x: Int, y) => x + y, (x: Int, y) => x + y).collect().foreach(println)
    //rdd.combineByKey(x => x, _ + _, _ + _).collect().foreach(println)

  }

}
