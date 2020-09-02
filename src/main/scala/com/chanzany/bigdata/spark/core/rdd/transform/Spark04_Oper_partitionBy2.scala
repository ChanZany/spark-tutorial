package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * 自定义分区器
 */
object Spark04_Oper_partitionBy2 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
    val sc = new SparkContext(config)

    //    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("andy", 1), ("bob", 2), ("candy", 3)))
    val partRdd: RDD[(String, Int)] = rdd.partitionBy(new MyPartitioner(3))
    partRdd.saveAsTextFile("output")


  }

}

//声明分区器
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  //  override def getPartition(key: Any): Int = {
  //    1 //全部放在1号分区
  //  }
  override def getPartition(key: Any): Int = {
    key.toString.charAt(0) match {
      case 'a' => 0
      case 'b' => 1
      case 'c' => 2
    }
  }
}
