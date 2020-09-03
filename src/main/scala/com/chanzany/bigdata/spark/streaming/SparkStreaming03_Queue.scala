package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable

/**
 * 测试过程中，可以通过使用ssc.queueStream(queueOfRDDs)来创建DStream，
 * 每一个推送到这个队列中的RDD，都会作为一个DStream处理。
 */
object SparkStreaming03_Queue {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_Queue")
    val ssc = new StreamingContext(sparkConf, Milliseconds(3000))
    //ssc.start()
    //java.lang.IllegalArgumentException:
    // requirement failed: No output operations registered, so nothing to execute
    // 对于sparkStreaming这种流式处理框架，需要在启动采集之前完成业务逻辑的封装
    // 这样才能实现来一条数据处理一条数据，所以ssc.start一定要放在业务逻辑之后

    val queue = new mutable.Queue[RDD[String]]()
    val queueDS: InputDStream[String] = ssc.queueStream(queue)

    queueDS.print()


    ssc.start()
    // 模拟消息队列(RDD)
    for (i <- 1 to 5) {
      val rdd = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)
    }

    ssc.awaitTermination()
  }
}
