package org.example

import base.FlinkBase
import streaming.AlarmStreaming.properties

import scala.reflect.runtime.{universe => ru}
object Te {
  def main(args: Array[String]): Unit = {
    // 使用类路径创建类的实例
    val flinkBase: FlinkBase = Class.forName("imp.GeelyProcess").newInstance().asInstanceOf[FlinkBase]
    print(flinkBase)
  }

}
