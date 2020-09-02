package com.chanzany.statisticProject.application

import com.chanzany.statisticProject.controller.{Top10AnalysisController, Top10SessionAnalysisController}
import com.chanzany.summerFrame.core.TApplication

/**
 * 统计热门品类前十
 */
object Top10SessionAnalysisApp extends App with TApplication{

  //TODO 统计Top10热门品类
  start("start"){
    val controller = new Top10SessionAnalysisController
    controller.execute()
  }

}
