package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_map {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //    //map算子
//    val listRdd: RDD[Int] = sc.makeRDD(1 to 10, 2)
    //    //    val resRdd: RDD[Int] = listRdd.map(x => x * 2)
    //    val resRdd: RDD[Int] = listRdd.map(_ * 2)
    //    resRdd.collect().foreach(println)

    //mapPartitions可以对一个RDD中的所有分区进行遍历
    //减少了计算任务的分发，也就是减少了Master和executor的交互次数，所以效率优于map算子
    //但是存在OOM内存溢出问题
    //    val mpRdd: RDD[Int] = listRdd.mapPartitions(partition_n => {
    //      partition_n.map(_*2) //这个map是scala中集合的map函数
    //    })
    //    mpRdd.collect().foreach(println)


    //    //mapPartitionsWithIndex(func)
    //    val tupleRdd: RDD[(Int, String)] = listRdd.mapPartitionsWithIndex {
    //      case (num, datas) => {
    //        datas.map((_, "分区号:" + num))
    //      }
    //    }
    //
    //    tupleRdd.collect().foreach(println)

//    //flatMap:扁平化映射
//    val arrRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
//    val resRdd: RDD[Int] = arrRdd.flatMap(list => list)
//    resRdd.collect().foreach(println)




  }

}
