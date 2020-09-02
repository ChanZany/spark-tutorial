package com.chanzany.statisticProject.application

import com.chanzany.statisticProject.controller.WordCountController
import com.chanzany.summerFrame.core.TApplication


object WordCountApp extends App with TApplication {

  start("Spark") {
    val controller = new WordCountController()
    controller.execute()

  }


}
