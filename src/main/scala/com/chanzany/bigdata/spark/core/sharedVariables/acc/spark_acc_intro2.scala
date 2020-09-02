package com.chanzany.bigdata.spark.core.sharedVariables.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object spark_acc_intro2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ))
    /**
     * TODO 累加器：分布式共享只写变量
     *  所谓的累加器：一般作用为累加（数据求和，数据累加）数据
     *  工作流程：
     *     1. 将累加器变量注册到spark中
     *     2. 执行计算时，spark会将累加器发送到Executor执行计算
     *     3. 计算完毕后，Executor会将累加器计算的结果返回到Driver中
     *     4. Driver端获取到多个累加器的结果，然后两两合并，最后得到累加器的执行结果
     */



    //TODO 声明累加器变量Accumulator
    var sum: LongAccumulator = sc.longAccumulator("sum")
    //    sc.doubleAccumulator()
    //    sc.collectionAccumulator()
    rdd.foreach {
      case (word, count) => {
        //TODO 使用累加器
        sum.add(count)
      }
    }
    // TODO 获取累加器的结果
    println(s"a,${sum.value}") //a,10

    sc.stop()
  }
}
