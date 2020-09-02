package com.chanzany.statisticProject.service

import com.chanzany.statisticProject.bean
import com.chanzany.statisticProject.dao.PageFlowDao
import com.chanzany.summerFrame.core.TService
import org.apache.spark.rdd.RDD

class PageFlowService extends TService {
  private val pageFlowDao = new PageFlowDao

  def analysis1(): Any = {
    //TODO 获取原始用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = pageFlowDao.getUserVisitAction("input/user_visit_action.txt")
    actionRDD.cache()
    //TODO 计算分母
    val pageToOneRDD: RDD[(Long, Int)] = actionRDD.map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    //TODO 计算分子

    //TODO 将数据根据用户session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
      actionRDD.groupBy(_.session_id)

    //TODO 按时间进行升序排列，并按特定格式输出(sessionId,[(page1-page2,1),(page2-page3,1)])
    val _pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //TODO 将分组后的数据根据时间排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )

        //TODO 将排序后的数据进行结构的转换
        // action => pageId (1,2,3,4)
        val pageIds: List[Long] = actions.map(_.page_id)

        //TODO 将pageIds转换为(1-2,1),(2-3,1),(3-4,1)
        val zipIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        zipIds.map {
          case (pageId1, pageId2) => {
            (pageId1 + "-" + pageId2, 1)
          }
        }
      }
    )
    //TODO 将分组后的数据进行结构的转换
    // [(page1-page2,1),(page2-page3,1)],
    // [(page1-page2,1),(page2-page3,1)],
    // [(page1-page2,1),(page2-page3,1)]
    // =>  (page1-page2,1),(page2-page3,1),(page1-page2,1),(page2-page3,1),
    // (page1-page2,1),(page2-page3,1)]
    val pageIdSumRDD: RDD[List[(String, Int)]] = _pageFlowRDD.map(_._2)
    val pageFlowRDD: RDD[(String, Int)] = pageIdSumRDD.flatMap(list => list)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowRDD.reduceByKey(_ + _)


    //TODO 计算页面单跳转换率
    //(1-2)/1
    pageFlowToSumRDD.foreach {
      case (pageFlow, sum) => {
        val pageId: Long = pageFlow.split("-")(0).toLong
        val value: Int = pageCountArray.toMap.getOrElse(pageId, -1)
        println("页面跳转【" + pageFlow + "】的转换率为" + (sum.toDouble / value))
      }
    }

  }

  /**
   * 对指定的页面流程进行页面单跳转换率的统计
   * 1，2，3，4，5，6，7
   * 1-2，2-3，3-4，4-5，5-6，6-7
   *
   * @return
   */
  override def analysis(): Any = {

    val flowIds = List(1, 2, 3, 4, 5, 6, 7)
    val objectFlowIds: List[String] = flowIds.zip(flowIds.tail).map(t => t._1 + "-" + t._2)

    //TODO 获取原始用户行为数据
    val actionRDD: RDD[bean.UserVisitAction] = pageFlowDao.getUserVisitAction("input/user_visit_action.txt")
    actionRDD.cache()

    //TODO 计算分母

    //TODO 将数据进行过滤后再进行统计
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(
      action => {
        flowIds.init.contains(action.page_id.toInt) //7不做分母
      }
    )

    val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
      action => {
        (action.page_id, 1)
      }
    )

    val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_ + _)
    val pageCountArray: Array[(Long, Int)] = pageToSumRDD.collect()

    //TODO 计算分子

    //TODO 将数据根据用户session进行分组
    val sessionRDD: RDD[(String, Iterable[bean.UserVisitAction])] =
      actionRDD.groupBy(_.session_id)

    //TODO 按时间进行升序排列，并按特定格式输出(sessionId,[(page1-page2,1),(page2-page3,1)])
    val _pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      iter => {
        //TODO 将分组后的数据根据时间排序
        val actions: List[bean.UserVisitAction] = iter.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )

        //TODO 将排序后的数据进行结构的转换
        // action => pageId (1,2,3,4)
        val pageIds: List[Long] = actions.map(_.page_id)
        /*不能在统计单跳前过滤
        pageIds.filter(id=>{
          flowIds.contains(id)
        })*/

        //TODO 将pageIds转换为(1-2,1),(2-3,1),(3-4,1)
        val zipIds: List[(Long, Long)] = pageIds.zip(pageIds.tail)
        zipIds.map {
          case (pageId1, pageId2) => {
            (pageId1 + "-" + pageId2, 1)
          }
        }.filter {
          case (ids, one) => {
            objectFlowIds.contains(ids)
          }
        }
      }
    )
    //TODO 将分组后的数据进行结构的转换

    val pageIdSumRDD: RDD[List[(String, Int)]] = _pageFlowRDD.map(_._2)
    val pageFlowRDD: RDD[(String, Int)] = pageIdSumRDD.flatMap(list => list)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowRDD.reduceByKey(_ + _)


    //TODO 计算页面单跳转换率
    //(1-2)/1
    pageFlowToSumRDD.foreach {
      case (pageFlow, sum) => {
        val pageId: Long = pageFlow.split("-")(0).toLong
        val value: Int = pageCountArray.toMap.getOrElse(pageId, -1)
        println("页面跳转【" + pageFlow + "】的转换率为" + (sum.toDouble / value))
      }
    }

  }

  override def analysis(data: Any): Any = ???
}
