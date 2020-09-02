package com.chanzany.summerFrame.core

import java.net.{ServerSocket, Socket}

import com.chanzany.summerFrame.util.{EnvUtils, PropertiesUtil}

/**
 * 主程序，是个特质，只需要传递执行的逻辑，获取环境和关闭环境自动完成
 *
 * 开发原则：OCP Open-Close(开闭原则)
 *    open:开发的程序代码应该对功能扩展开放
 *    close:在扩展的同时不应该对原有的代码进行修改
 */
trait TApplication {

  //初始化环境
  var envData: Any = null

  /**
   * 启动应用
   * 1. 函数柯里化
   * 2. 控制抽象
   *
   * @param t 参数类型：jdbc,file.hive,kafka,socket,serverSocket
   * @param op
   */
  def start(t: String)(op: => Unit) = {
    // TODO 1. 初始化缓存
    if (t == "Socket") {
      envData = new Socket(
        PropertiesUtil.getValue("server.host"),
        PropertiesUtil.getValue("server.port").toInt)
    } else if (t == "ServerSocket") {
      envData = new ServerSocket(
        PropertiesUtil.getValue("server.port").toInt
      )
    } else if (t == "Spark") {
      envData = EnvUtils.getEnv()
    }

    //TODO 2.业务逻辑
    try {
      op
    } catch {
      case ex: Exception => println("业务执行失败:" + ex.getMessage)
    }

    //TODO 3.环境关闭
    if (t == "ServerSocket") {
      val ServerSocket = envData.asInstanceOf[ServerSocket]
      if (!ServerSocket.isClosed) {
        ServerSocket.close()
      }
    } else if (t == "Socket") {
      val socket = envData.asInstanceOf[Socket]
      if (!socket.isClosed) {
        socket.close()
      }
    } else if (t == "Spark") {
      EnvUtils.clean()
    }

  }

}
