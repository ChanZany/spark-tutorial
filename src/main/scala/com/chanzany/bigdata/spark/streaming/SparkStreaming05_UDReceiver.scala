package com.chanzany.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Milliseconds, StreamingContext}



/**
 * 自定义数据采集器
 */
object SparkStreaming05_UDReceiver {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_UDReceiver")
    val ssc = new StreamingContext(sparkConf, Milliseconds(3000))

    val myReceiver = new MyReceiver("localhost",9999)
    val dStream: ReceiverInputDStream[String] = ssc.receiverStream(myReceiver)

    dStream.print()


    ssc.start()

    ssc.awaitTermination()
  }

  //TODO 自定义数据采集器
  // 1. 继承Receiver,定义泛型并传入Receiver指定参数StorageLevel
  // 2. 重写方法 onStart,onStop
  class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var socket: Socket = _

    def receive(): Unit ={
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "utf-8"
        )
      )

      var s:String = null
      while (true){
        s = reader.readLine()
        if(s!=null){
          //TODO 将获取的数据保存到框架内部进行封装
          store(s)
        }
      }

    }

    //TODO 采集器的启动
    override def onStart(): Unit = {
      socket = new Socket(host, port)
      new Thread("Socket Receiver") {
        setDaemon(true)
        override def run() { receive() }
      }.start()
    }

    //TODO 采集器的停止
    override def onStop(): Unit = {
      synchronized {
        if (socket != null) {
          socket.close()
          socket = null
        }
      }
    }
  }

}
