package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc = new SparkContext(config)

    /**
     * 需求: 创建一个 pairRDD，取出每个分区相同key对应值的最大值，然后相加
     */
    val rdd: RDD[(String, Double)] = sc.parallelize(List(("a", 3.0), ("a", 2.0), ("c", 4.0), ("b", 3.0), ("c", 6.0), ("c", 8.0)), 2)
//        val resRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
//        resRdd.collect().foreach(println)
//    getAvg(rdd)

  }

  /**
   * 练习: 计算每个 key 的平均值
   */
  def getAvg(rdd: RDD[(String, Double)]): Unit = {

    val groupRdd = rdd.groupByKey()
    val resRdd = groupRdd.map(g => (g._1,g._2.sum / g._2.size))
    resRdd.collect().foreach(println)
  }

}
