package com.chanzany.bigdata.spark.core.rdd.transform.KVRddCase

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
 * 样本如下：
 * 1516609143867 6 7 64 16
 * 1516609143869 9 4 75 18
 * 1516609143869 1 7 87 12
 *
 * 统计出每一个省份广告被点击次数的 TOP3
 */
object caseDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Case1")
    val sc = new SparkContext(conf)

    //TODO 1. 获取原始数据
    val dataRDD: RDD[String] = sc.textFile("src/main/scala/com/chanzany/bigdata/spark/core/KVRddCase/agent.log")

    //TODO 2. 将原始数据进行结构转换(数据清洗)，方便统计-->(省份-广告,1)
    val mapRDD: RDD[(String, Int)] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )

    //TODO 3. 将相同Key的数据进行分组聚合(省份-广告,sum)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //TODO 4. 将聚合后的结果进行结构的转换(省份,(广告,sum))
    val restructRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("-")
        (keys(0), (keys(1), sum))
      }
    }

    //TODO 5. 将相同省份的数据分在一个组中(省份,Iterator[(广告1,sum1),(广告2,sum2)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = restructRDD.groupByKey()

    //TODO 6. 将分组后的数据进行排序(降序)，取前3
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortWith(
        (left, rigth) => {
          left._2 > rigth._2
        }
      ).take(3)
    })

    //TODO 7. 将数据采集到控制台打印
    sortRDD.collect().foreach(println)

    sc.stop()
  }

}
