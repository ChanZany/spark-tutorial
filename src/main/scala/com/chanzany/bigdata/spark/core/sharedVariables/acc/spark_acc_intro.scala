package com.chanzany.bigdata.spark.core.sharedVariables.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_acc_intro {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String,Int)] = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ))
//    val sum= rdd.reduceByKey(_ + _)
    // 不用shuffle的情况下完成reduceByKey的功能
    var sum = 0
    rdd.foreach{
      case (word,count)=>{
        sum = sum+count
        println(sum)
      }
    }

    println(s"a,$sum") //a,0

    sc.stop()
  }
}
