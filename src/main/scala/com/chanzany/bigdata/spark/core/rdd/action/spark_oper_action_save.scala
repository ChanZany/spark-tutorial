package com.chanzany.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_oper_action_save {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("actionRDD")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO save相关算子:将RDD中的数据以指定格式和路径进行保存
    rdd.saveAsTextFile("save/demo1")
    rdd.saveAsObjectFile("save/demo2")
    rdd.map((_,1)).saveAsSequenceFile("save/demo3")
//    rdd.map((_,1)).saveAsHadoopFile("hdfs://hadoop101:/save/demo3")



    sc.stop()

  }
}
