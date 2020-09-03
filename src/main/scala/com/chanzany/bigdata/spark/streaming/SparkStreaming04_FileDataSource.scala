package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable

/**
文件数据流：能够读取所有HDFS API兼容的文件系统文件，
通过fileStream方法进行读取，Spark Streaming 将会监控dataDirectory目录并不断处理移动进来的文件，
记住目前不支持嵌套目录。
 */
object SparkStreaming04_FileDataSource {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming04_FileDataSource")
    val ssc = new StreamingContext(sparkConf, Milliseconds(3000))

    val dirDS: DStream[String] = ssc.textFileStream("in")
    dirDS.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
