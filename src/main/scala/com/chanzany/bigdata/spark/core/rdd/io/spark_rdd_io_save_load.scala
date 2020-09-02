package com.chanzany.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_rdd_io_save_load {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_io").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input/word.txt")
    rdd.saveAsTextFile("output/word_txt")

    rdd.saveAsObjectFile("output/word_obj")
    val objRDD: RDD[String] = sc.objectFile[String]("output/word_obj")
    println(objRDD.collect().mkString(","))

    rdd.map((_,1)).saveAsSequenceFile("output/word_sequenceFile")
    val seqRDD: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output/word_sequenceFile")
    println(seqRDD.collect().mkString(","))

    sc.stop()
  }
}
