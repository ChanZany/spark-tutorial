package com.chanzany.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming的窗口函数
 */
object SparkStreaming08_WindowOper {
  def main(args: Array[String]): Unit = {
    /**
     * scala中的滑动窗口函数
     * val ints  = List(1,2,3,4,5,6)
     * val newIter: Iterator[List[Int]] = ints.sliding(2,2) //参数1 size 参数2 step
     * for (list <- newIter) {
     * println(list.mkString(","))
     * }
     * ----------------
     * 1,2
     * 3,4
     * 5,6
     */


    val sparkConf: SparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("SparkStreaming08_WindowOper")



    //采集周期为3s
    val streamContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        streamContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaParams)
      )
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    //TODO 使用SparkStreaming的窗口
    // 参数1：窗口大小（应该是采集周期的整数倍）
    // 参数2：滑动步长 (同样应该是采集周期的整数倍)
    val windowDStream: DStream[String] = valueDStream.window(Seconds(9), Seconds(3))

    val wordDStream: DStream[String] = windowDStream.flatMap(_.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    wordToSumDStream.print()

    streamContext.start()

    streamContext.awaitTermination()

  }


}
