package com.chanzany.statisticProject.service

import com.chanzany.statisticProject.bean
import com.chanzany.statisticProject.bean.HotCategory
import com.chanzany.statisticProject.dao.{Top10AnalysisDao, Top10SessionAnalysisDao}
import com.chanzany.statisticProject.helper.HotCategoryAccumulator
import com.chanzany.summerFrame.core.TService
import com.chanzany.summerFrame.util.EnvUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Top10SessionAnalysisService extends TService {
  private val dao = new Top10SessionAnalysisDao


  override def analysis(): Any = {

  }

  override def analysis(data: Any) = {
    val hotCategories: List[HotCategory] = data.asInstanceOf[List[HotCategory]]
    //    println(hotCategories.isEmpty)
    val hotCategorieIds: List[String] = hotCategories.map(_.categoryId)

    //TODO 使用广播变量实现数据的传递
    val bsList: Broadcast[List[String]] = EnvUtils.getEnv().broadcast(hotCategorieIds)

    //TODO 获取用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = dao.getUserVisitAction("input/user_visit_action.txt")

    //TODO 对数据进行过滤
    // 对用户的点击行为进行过滤
    // 1) 点击的品类id不为-1
    // 2）点击的品类id要等于Top10品类ID
    //DEBUG println(actionRDD.isEmpty())
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
//          var flag = false
//          hotCategories.foreach(
//            hc => {
//              if (hc.categoryId.toLong == action.click_category_id) {
//                flag = true
//              }
//            }
//          )
//          flag
          bsList.value.contains(action.click_category_id.toString)

        } else {
          false
        }
      }
    )

    //DEBUG  println(filterRDD.isEmpty())

    //TODO 将过滤后得数据进行处理
    // (品类_Session,1) =>(品类_Session,sum)
    val pairRDD: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    )
    val reduceRDD: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)

    //TODO 将统计后的结果做结构的转换
    // (品类_Session,sum) => (品类,(Session,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, count) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), count))
      }
    }

    //TODO 将转换结构后的数据对品类进行分组
    // (品类,Iterator[(Session1,sum1),(Session2,sum2)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //TODO 将分组后的数据排序取前十名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => iter.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(10)
    )

    resultRDD.collect()
  }


}
