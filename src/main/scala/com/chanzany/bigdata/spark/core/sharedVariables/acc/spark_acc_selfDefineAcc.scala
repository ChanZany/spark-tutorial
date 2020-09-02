package com.chanzany.bigdata.spark.core.sharedVariables.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark_acc_selfDefineAcc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_selfDefineAcc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 累加器：WordCount

    val wordRDD: RDD[String] = sc.makeRDD(List(
      "hello scala","hello spark","hello world","hello java"
    ))

    val acc = new MyWordCountAccumulator
    sc.register(acc)
    wordRDD.flatMap(_.split(" ")).foreach{
      word=>{
        acc.add(word)
      }
    }

    println(acc.value)


    sc.stop()
  }
  //TODO 自定义累加器
  // 1. 创建累加器:
  //    1) 继承AccumulatorV2
  //    2) 定义泛型：IN（累加器输入的值类型） OUT(累加器返回结果类型)
  //    3) 重写方法(6)
  // 2. 注册累加器(sc.register)

  class MyWordCountAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
    //存储word-count的集合
    var wordCountMap = mutable.Map[String,Int]()

    /**
     * 累加器是否初始化
     * @return
     */
    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    /**
     * 复制累加器
     * @return
     */
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyWordCountAccumulator
    }

    /**
     * 重置累加器
     */
    override def reset(): Unit = {
      wordCountMap.clear()
    }

    /**
     * 向累加器中增加值
     * word-count
     * @param word
     */
    override def add(word: String): Unit = {
      wordCountMap(word)=wordCountMap.getOrElse(word,0) + 1
    }

    /**
     * 累加器之间的合并(Driver端)
     * @param other
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap = map1.foldLeft(map2)(
        (map,kv)=>{
          map(kv._1) = map.getOrElse(kv._1,0)+kv._2
          map
        }
      )

    }

    /**
     * 返回累加器的值
     * @return
     */
    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }
}
