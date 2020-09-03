package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 需要启动netCat:`nc -lp 9999`
 */
object SparkStreaming02_WordCount1 {
  def main(args: Array[String]): Unit = {
    //TODO Spark环境
    // 使用"local"只有一个线程在启用，而sparkStreaming要起作用至少的有两个线程
    // (executor(采集器+处理数据)，driver任务调度)
    //val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkStreaming01_WordCount")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming01_WordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_, 1))
    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_ + _)
    wordToSumDS.print()
//    wordToSumDS.foreachRDD(rdd =>{rdd.collect})

    //TODO 启动采集器
    ssc.start()
    //TODO 等待采集器的结束
    ssc.awaitTermination()
  }
}
