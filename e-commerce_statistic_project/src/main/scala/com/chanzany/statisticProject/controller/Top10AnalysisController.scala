package com.chanzany.statisticProject.controller

import com.chanzany.statisticProject.service.Top10AnalysisService
import com.chanzany.summerFrame.core.TController

class Top10AnalysisController extends TController{
  private val service = new Top10AnalysisService
  override def execute(): Unit = {
    val result = service.analysis()
    result.foreach(println)
  }
}
