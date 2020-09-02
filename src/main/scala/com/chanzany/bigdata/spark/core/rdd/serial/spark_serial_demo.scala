package com.chanzany.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_serial_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("serial").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO Spark序列化
    /*val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.foreach(
      num=>{
        val user = new User()
        println("age="+(user.age+num))
      }
    )*/

    //TODO SparkException: Task not serializable，
    // 原因 Caused by: java.io.NotSerializableException: serial.spark_serial_demo$User
    // 如果算子中使用了算子以外的对象(变/常量)，那么在执行时，需要保证这个对象能序列化。
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    val user = new User() //Driver中的变量
    //    rdd.foreach(
    //      num => {
    //        println("age=" + (user.age + num)) //Executor中执行
    //      }
    //    )
    // TODO Scala闭包检测
    val rdd: RDD[Int] = sc.makeRDD(List())
    val user = new User() //Driver中的变量
    // Spark算子的操作其实都是闭包，所以闭包可能包含外部的变量
    // 如果包含外部的变量，那么就一定要保证这个外部变量可序列化
    // 所以spark在提交作业前，应该对闭包内的变量进行检测，检测闭包内使用的变量皆可序列化
    rdd.foreach(
      num => {
        println("age=" + (user.age + num)) //Executor中执行
      }
    )

  }

  //  class User {
  //    val age: Int = 20
  //  }

  //  class User extends Serializable {
  //    val age: Int = 20
  //  }

  // 样例类自动继承了Serializable特质
  //  case class User(age: Int = 20)

  class User {
    val age: Int = 20
  }

}
