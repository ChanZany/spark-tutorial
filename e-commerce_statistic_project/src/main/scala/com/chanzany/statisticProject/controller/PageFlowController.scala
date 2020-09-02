package com.chanzany.statisticProject.controller

import com.chanzany.statisticProject.service.PageFlowService
import com.chanzany.summerFrame.core.TController

class PageFlowController extends TController{
  private val pageFlowService = new PageFlowService
  override def execute(): Unit = {
    pageFlowService.analysis()
  }
}
