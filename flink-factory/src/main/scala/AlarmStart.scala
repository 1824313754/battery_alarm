import base.{AlarmCountFunction, AlarmStreaming, BatteryStateFunction}

object AlarmStart extends AlarmStreaming{
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
        streaming.setProperties(streaming.getConfig(args))
        //核心处理类的实现
        streaming.setBatteryProcessFunction(alarmProcess)
        //报警计数类的实现
        streaming.setAlarmCountClass(alarmCount)
        streaming.run()
    }



}
