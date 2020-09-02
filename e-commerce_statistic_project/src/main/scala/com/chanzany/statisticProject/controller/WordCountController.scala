package com.chanzany.statisticProject.controller

import com.chanzany.statisticProject.service.WordCountService
import com.chanzany.summerFrame.core.TController

/**
 * wordCount控制器
 */
class WordCountController extends TController{
  private val wordCountService = new WordCountService

  override def execute(): Unit = {
    val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
    println(wordCountArray.mkString(","))

  }
}
