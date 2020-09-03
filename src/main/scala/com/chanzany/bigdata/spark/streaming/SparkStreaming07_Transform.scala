package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * SparkStreaming与Kafka的对接
 */
object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Transform")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //TODO 转换

    //Code Driver(1)
    val newDS: DStream[String] = ds.transform(
      rdd => {
        //Code Driver(N):周期性执行
        rdd.map(
          data => {
            //Code Executor
            data * 2
          })
      })

    //Code Driver(1)
    val newDS2: DStream[String] = ds.map(
      data => {
        //Code Executor
        data * 2
      })


    newDS.print()


    ssc.start()

    ssc.awaitTermination()
  }


}
