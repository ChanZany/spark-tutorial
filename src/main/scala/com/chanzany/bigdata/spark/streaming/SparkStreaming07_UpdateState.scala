package com.chanzany.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 有状态数据统计
 * 1. 设置检查点(保存阶段数据)
 * 2. 使用updateStateByKey进行不同阶段数据的聚合操作
 */
object SparkStreaming07_UpdateState {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_UpdateState")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //TODO 保存数据的状态，需要设定检查点路径
    ssc.sparkContext.setCheckpointDir("ckpt")

    //TODO 从kafka中采集数据
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaParams)
      )

    //TODO 对采集的数据进行WordCount操作
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    val wordDStream: DStream[String] = valueDStream.flatMap(_.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //TODO 使用updateStateXX进行有状态数据转换操作
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    stateDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }


}
