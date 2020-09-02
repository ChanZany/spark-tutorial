package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //创建RDD
    //1）从内存中创建makeRDD,底层实现就是parallelize
    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    listRdd.collect().foreach(println)
    //2）从内存中创建parallelize
    val arrRdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4), 2) //设置分区数为2
    arrRdd.collect().foreach(println)
    //测试分区
    //    listRdd.saveAsTextFile("output")

    //3）从外部存储系统的数据集创建
    // 默认情况下，可以读取项目路径，也可以读取其他路径：HDFS
    // 默认从文件中读取的数据都是字符串类型
    val fileRdd: RDD[String] = sc.textFile("input", 2) //设置【最小】分区数是2，结果取决于hadoop读取文件分片规则
//    val hdfsRdd: RDD[String] = sc.textFile("hdfs://hadoop101:9000/spark/test")
    //    fileRdd.collect().foreach(println)

    //4）从其他 RDD 转换得到新的 RDD
    val transferRdd: RDD[String] = fileRdd.flatMap(line => line.split("\\W"))

  }

}
