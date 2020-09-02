package com.chanzany.statisticProject.service

import com.chanzany.statisticProject.dao.WordCountDao
import com.chanzany.summerFrame.core.TService
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  override def analysis(): Array[(String, Int)] = {
    val fileRDD: RDD[String] = wordCountDao.readFile("input/word.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val wordCountArray: Array[(String, Int)] = reduceRDD.collect()
    wordCountArray
  }

  override def analysis(data: Any): Any = {
    null
  }
}
