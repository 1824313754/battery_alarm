import base.{AlarmCountFunction, AlarmStreaming, BatteryStateFunction}

object AlarmStart {
  def main(args: Array[String]): Unit = {
    val streaming = new AlarmStreaming()
    val tool = streaming.getConfig(args)
    val alarmProcessName = tool.get("alarm.process")
    val alarmCountName = tool.get("alarm.count")
    //报警规则类
    val alarmProcess = Class.forName(alarmProcessName).newInstance().asInstanceOf[BatteryStateFunction]
    //报警计数类
    val alarmCount = Class.forName(alarmCountName).newInstance().asInstanceOf[AlarmCountFunction]
    //设置配置文件
    streaming.setProperties(tool)
      //设置任务名称
      .setJobName(alarmProcessName.split("\\.")(0))
      //核心处理类的实现
      .setBatteryProcessFunction(alarmProcess)
      //报警计数类的实现
      .setAlarmCountClass(alarmCount)
      //开始执行任务
      .run()
  }

}
