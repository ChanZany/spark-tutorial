package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {
    //TODO Spark -WordCount
    /**
     * spark是一个计算框架
     * 开发人员使用Spark框架的API实现计算功能
     */
    //TODO 1. 准备Spark环境
    //setMaster:设定Spark环境的位置，打包的时候需要去掉
    val sparkConf = new SparkConf().setAppName("wordCount")

    //TODO 2. 建立与Spark的连接
    val context = new SparkContext(sparkConf)

    //TODO 3. 实现业务操作
    //TODO 3.1 读取指定目录下的数据文件(多个)
    // 参数path可以指向单一文件也可以指向文件目录
    // RDD:更适合并行计算的数据模型
    val fileRDD: RDD[String] = context.textFile(args(0))

    //TODO 3.2 将读取的内容进行扁平化操作(切分word)
    val wordRDD: RDD[String] = fileRDD.flatMap(line => {
      line.split("\\W")
    })

    //TODO 3.3 将分词后的数据进行结构的转换{word=>(word,1)}
    val mapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))

    //TODO 3.4 将转换结构后的数据根据key:word进行分组聚合
    val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)


    //TODO 3.5 将聚合的结果打印到控制台(或者写入一个新的文件)
    val wordCountTuples: Array[(String, Int)] = wordToSumRDD.collect()
    wordCountTuples.foreach(println)

    //TODO 4. 释放连接
    context.stop()

  }

}
