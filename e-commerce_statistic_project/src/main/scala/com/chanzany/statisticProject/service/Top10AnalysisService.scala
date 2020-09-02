package com.chanzany.statisticProject.service

import com.chanzany.statisticProject.bean.HotCategory
import com.chanzany.statisticProject.dao.Top10AnalysisDao
import com.chanzany.statisticProject.helper.HotCategoryAccumulator
import com.chanzany.summerFrame.core.TService
import com.chanzany.summerFrame.util.EnvUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Top10AnalysisService extends TService {
  private val dao = new Top10AnalysisDao

  def analysis1() = {
    val fileRDD: RDD[String] = dao.readFile("input/user_visit_action.txt")
    //TODO 对品类进行点击的统计(category,clickCount)
    val clickRDD: RDD[(String, Int)] = fileRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        (fields(6), 1) //取出点击品类字段，并形成(category,1)
      }
    ).filter(_._1 != "-1") //过滤出合法数据
    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)

    //TODO 对品类进行下单的统计(category,orderCount)
    // 下单的数据可能同时包含多个品类(","分隔)
    // 如：(品类1，品类2，品类3，10)，需要转换为=>(品类1,10),(品类2,10),(品类3,10)
    val orderRDD = fileRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        fields(8) //取出下单品类字段
      }
    ).filter(_ != "null") //过滤出合法数据
    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap {
      categories => {
        val ids: Array[String] = categories.split(",")
        ids.map((_, 1))
      }
    }
    val categoryIdToOrderCountRDD = orderToOneRDD.reduceByKey(_ + _)

    //TODO 对品类进行支付的统计(category,payCount)
    // 逻辑与下单一致
    val payToOneRDD = fileRDD
      .map(
        line => {
          val fields: Array[String] = line.split("_")
          fields(10) //取出支付品类字段
        }
      )
      .filter(_ != "null").flatMap {
      categories => {
        val ids: Array[String] = categories.split(",")
        ids.map((_, 1))
      }
    }
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_ + _)

    //TODO 将上面的统计结果转换结构 tuple3(clickCount,orderCount,payCount)
    // (品类,点击数量),(品类,下单数量),(品类,支付数量)=>(品类，(点击数量，下单数量，支付数量))
    val joinRDD: RDD[(String, ((Int, Int), Int))] = categoryIdToClickCountRDD.join(categoryIdToOrderCountRDD).join(categoryIdToPayCountRDD)
    //((Int, Int), Int))=>(Int, Int, Int)
    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD.mapValues {
      case ((clickCount, orderCount), payCount) => {
        (clickCount, orderCount, payCount)
      }
    }

    //TODO 将转换结果后的数据进行降序排序
    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, ascending = false)

    //TODO 将排序后的结果取前10名
    val resRDD: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    resRDD
  }

  def analysis2() = {
    //TODO 使用缓存优化
    val fileRDD: RDD[String] = dao.readFile("input/user_visit_action.txt").cache()
    //TODO 先过滤再map
    val clickRDD: RDD[(String, Int)] = fileRDD.filter(_.split("_")(6) != "-1").map(
      line => {
        val fields: Array[String] = line.split("_")
        (fields(6), 1)
      }
    )
    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)

    val orderRDD = fileRDD.filter(_.split("_")(8) != "null").map(
      line => {
        val fields: Array[String] = line.split("_")
        fields(8) //取出下单品类字段
      }
    )
    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap {
      categories => {
        val ids: Array[String] = categories.split(",")
        ids.map((_, 1))
      }
    }
    val categoryIdToOrderCountRDD = orderToOneRDD.reduceByKey(_ + _)

    val payToOneRDD = fileRDD
      .filter(_.split("_")(10) != "null")
      .map(
        line => {
          val fields: Array[String] = line.split("_")
          fields(10) //取出支付品类字段
        }
      )
      .flatMap {
        categories => {
          val ids: Array[String] = categories.split(",")
          ids.map((_, 1))
        }
      }
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_ + _)

    //TODO Join产生笛卡儿积效率低下
    // 使用聚合操作来优化
    // (品类,点击数量),(品类,下单数量),(品类,支付数量)
    // =>(品类,(点击数量,0,0)),(品类,(0,下单数量,0)),(品类,(0,0支付数量))
    // =>reduceByKey
    // =>(品类，(点击数量，下单数量，支付数量))
    val oneHotClickRDD: RDD[(String, (Int, Int, Int))] = categoryIdToClickCountRDD.mapValues((_, 0, 0))
    val oneHotOrderRDD: RDD[(String, (Int, Int, Int))] = categoryIdToOrderCountRDD.mapValues((0, _, 0))
    val oneHotPayRDD: RDD[(String, (Int, Int, Int))] = categoryIdToPayCountRDD.mapValues((0, 0, _))
    val countRDD: RDD[(String, (Int, Int, Int))] = oneHotClickRDD
      .union(oneHotOrderRDD)
      .union(oneHotPayRDD)
    val reduceCountRDD: RDD[(String, (Int, Int, Int))] = countRDD.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )


    //TODO 将转换结果后的数据进行降序排序
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceCountRDD.sortBy(_._2, ascending = false)

    //TODO 将排序后的结果取前10名
    val resRDD: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    resRDD
  }

  def analysis3() = {
    //TODO 减少聚合操作的次数来优化
    // 优化前：
    // fileRDD=>categoryIdToClickCountRDD=>oneHotClickRDD=>reduceByKey
    // =>(品类，(点击数量，下单数量，支付数量))
    // 优化后：
    // fileRDD=>(品类,(1,0,0)),(品类,(0,1,0)),(品类,(0,0,1))
    // =reduceByKey=>(品类，(点击数量，下单数量，支付数量))
    val fileRDD: RDD[String] = dao.readFile("input/user_visit_action.txt")

    val flatMapRDD: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(
      line => {
        val fields = line.split("_")
        if (fields(6) != "-1") {
          //点击行为
          List((fields(6), (1, 0, 0)))
        } else if (fields(8) != "null") {
          //下单行为
          val ids: Array[String] = fields(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (fields(10) != "null") {
          //支付行为
          val ids: Array[String] = fields(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )
    reduceRDD.sortBy(_._2, false).take(10)


  }

  def analysis4() = {
    //TODO 将相关操作封装到类中
    val fileRDD: RDD[String] = dao.readFile("input/user_visit_action.txt")
    // line =>
    //  click= HotCategory(1,0,0)
    //  order= HotCategory(0,1,0)
    //  pay= HotCategory(0,0,1)
    val flatMapRDD: RDD[(String, HotCategory)] = fileRDD.flatMap(
      line => {
        val fields = line.split("_")
        if (fields(6) != "-1") {
          //点击行为
          List((fields(6), HotCategory(fields(6), 1, 0, 0)))
        } else if (fields(8) != "null") {
          //下单行为
          val ids: Array[String] = fields(8).split(",")
          ids.map(id => (id, HotCategory(id, 0, 1, 0)))
        } else if (fields(10) != "null") {
          //支付行为
          val ids: Array[String] = fields(10).split(",")
          ids.map(id => (id, HotCategory(id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val reduceRDD = flatMapRDD.reduceByKey(
      (c1, c2) => {
        c1.clickCount = c1.clickCount + c2.clickCount
        c1.orderCount = c1.orderCount + c2.orderCount
        c1.payCount = c1.payCount + c2.payCount
        c1
      }
    )
    reduceRDD.collect().sortWith(
      (left, right) => {
        val leftHC = left._2
        val rightHC = right._2
        if (leftHC.clickCount>rightHC.clickCount){
          true
        }else if (leftHC.clickCount==rightHC.clickCount){
          if (leftHC.orderCount>rightHC.orderCount){
            true
          }else if(leftHC.orderCount == rightHC.orderCount){
            if(leftHC.payCount>rightHC.payCount){
              true
            }else{
              false
            }
          }else{
            false
          }
        }else{
          false
        }
      }
    ).take(10)


  }
  override def analysis() = {
    //TODO 使用累加器来取代reduceByKey对数据进行聚合
    val fileRDD: RDD[String] = dao.readFile("input/user_visit_action.txt")
    val acc = new HotCategoryAccumulator
    EnvUtils.getEnv().register(acc,"hotCategory")
    //TODO 将数据遍历，放入累加器中
    fileRDD.foreach(
      line =>{
        val fields: Array[String] = line.split("_")
        if(fields(6) != "-1"){//点击
          acc.add(fields(6),"click")
        }else if (fields(8) != "null"){//下单
          val cids: Array[String] = fields(8).split(",")
          cids.foreach(
            id=>acc.add((id,"order"))
          )
        }else if (fields(10) != "null"){//支付
          val cids: Array[String] = fields(10).split(",")
          cids.foreach(
            id=>acc.add((id,"pay"))
          )
        }else{
          Nil
        }
      }
    )

    //TODO 获取累加器的值
    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)
    categories.toList.sortWith(
      (left,right)=>{
        if(left.clickCount>right.clickCount){
          true
        }else if(left.clickCount == right.clickCount){
          if(left.orderCount>right.orderCount){true}
          else if(left.orderCount==right.orderCount){
            if(left.payCount>right.payCount){true}else{false}
          }else{false}
        }else{false}
      }
    ).take(10)

  }
  override def analysis(data: Any): Any = {

  }
}
