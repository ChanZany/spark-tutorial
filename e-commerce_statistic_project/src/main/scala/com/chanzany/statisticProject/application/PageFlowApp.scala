package com.chanzany.statisticProject.application

import com.chanzany.statisticProject.controller.PageFlowController
import com.chanzany.summerFrame.core.TApplication

object PageFlowApp extends App with TApplication{

  start("spark"){
    val controller = new PageFlowController
    controller.execute()
  }

}
