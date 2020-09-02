package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper_groupBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //生成数据，按照指定规则进行分组
    //分组后的数据形成了对偶元组(K-V),K表示分组的Key,V表示分组的数据集合
    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val groupsRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(i => i % 2)
//    groupsRdd.collect().foreach(println)


    val strRdd: RDD[String] = sc.makeRDD(List("chanzany", "chanzhi", "tom", "jessica", "jerry", "tiktok"))
    val resRdd: RDD[(Char, Iterable[String])] = strRdd.groupBy(i => i.charAt(0))
    resRdd.collect().foreach(println)



  }

}
