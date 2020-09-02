package com.chanzany.summerFrame.util

import org.apache.spark.{SparkConf, SparkContext}

object EnvUtils {
  //创建线程共享数据
  private val scLocal = new ThreadLocal[SparkContext]

  //获取环境对象
  def getEnv()={
    //从当前线程的共享空间中获取环境对象
    var sc: SparkContext = scLocal.get()
    if (sc == null){
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkApplication")
      sc = new SparkContext(conf)
      scLocal.set(sc)
    }
    sc
  }

  //释放资源销毁环境
  def clean(){
    getEnv().stop()
    scLocal.remove()
  }

}
