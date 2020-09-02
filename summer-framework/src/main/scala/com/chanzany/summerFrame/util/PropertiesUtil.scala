package com.chanzany.summerFrame.util

import java.util.ResourceBundle

object PropertiesUtil {
  //绑定配置文件
  val summer: ResourceBundle = ResourceBundle.getBundle("summers")
  def getValue(key:String): String ={
    summer.getString(key)
  }

}
