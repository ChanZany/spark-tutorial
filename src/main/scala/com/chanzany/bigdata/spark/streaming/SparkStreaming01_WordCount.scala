package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * 需要启动netCat:`nc -lp 9999`
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO Spark环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val ssc = new StreamingContext(sparkConf, Milliseconds(3000))

    //TODO 执行逻辑
    // 从socket获取数据是以行为单位的
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_, 1))
    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_ + _)
    wordToSumDS.print()

    //TODO 关闭
    // Driver程序执行streaming处理过程中不能结束
    // 采集器在正常情况下启动后不应该停止，除非特殊情况
    //ssc.stop()
    //启动采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()
  }
}
