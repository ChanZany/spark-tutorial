package com.chanzany.summerFrame.core

/**
 * 服务层，封装业务逻辑
 */
trait TService {
  //数据分析
  def analysis():Any
  def analysis(data:Any):Any

}
