package com.chanzany.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}



object SparkStreaming09_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Transform")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val socketLineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //转换
    //TODO 代码(driver)(1)
    //val a =1
    socketLineDStream.map{
      case x =>{
        //TODO 代码(Executor)(n)
        x
      }
    }

    //TODO 代码(Driver)(1)
    socketLineDStream.transform{
      case rdd =>{
        //TODO 代码(driver)(m=采集周期)
        rdd.map{
          case x=>{
            //TODO 代码 (Executor)(n)
            x
          }
        }
      }
    }






    ssc.start()

    ssc.awaitTermination()
  }


}
