package com.chanzany.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

/** join(otherDataset, [numTasks])
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的RDD
 */
object Spark04_Oper_join {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
    val sc = new SparkContext(config)

    val nameRdd = sc.parallelize(Array((1, "tom"), (1, "jerry"), (2, "bob")))
    val ageRdd = sc.parallelize(Array((1, 12), (3, 14), (2, 25)))
    nameRdd.join(ageRdd).collect().foreach(println)

  }

}
