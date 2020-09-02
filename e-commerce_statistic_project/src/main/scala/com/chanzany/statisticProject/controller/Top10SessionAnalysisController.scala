package com.chanzany.statisticProject.controller

import com.chanzany.statisticProject.bean
import com.chanzany.statisticProject.service.{Top10AnalysisService, Top10SessionAnalysisService}
import com.chanzany.summerFrame.core.TController

class Top10SessionAnalysisController extends TController {
  private val categoryService = new Top10AnalysisService
  private val sessionService = new Top10SessionAnalysisService

  override def execute(): Unit = {
    val categories: List[bean.HotCategory] = categoryService.analysis()
    val result = sessionService.analysis(categories)
    result.foreach(println)

  }
}
