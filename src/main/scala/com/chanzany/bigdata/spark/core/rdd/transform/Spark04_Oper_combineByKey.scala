package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey其实是aggregateByKey的简化操作，seqop和combop相同
 */
object Spark04_Oper_combineByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foldByKey")
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array(
      ("a", 3), ("a", 2), ("c", 4),
      ("b", 3), ("c", 6), ("c", 8),
      ("a", 4), ("a", 6)))

    val resRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      //a:3==>a:(3,1)
      x => (x, 1),
      // a:(3,1),a:2==>a:(3+2,1+1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      //a:(5,2),a:(10,2)==>a:(5+10,2+2)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    resRdd.collect().foreach(println)

  }

}
