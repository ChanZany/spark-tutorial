package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey其实是aggregateByKey的简化操作，seqop和combop相同
 */
object Spark04_Oper_foldByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foldByKey")
    val sc = new SparkContext(config)
    //使用foldByKey算子，计算相同key对应值的相对结果
    val rdd = sc.parallelize(Array(("a",3), ("a",2), ("c",4), ("b",3), ("c",6), ("c",8)))
    val resRdd: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    resRdd.collect().foreach(println)

  }

}
