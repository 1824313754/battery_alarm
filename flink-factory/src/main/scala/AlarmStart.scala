import base.AlarmStreaming
import ruichi.{RuichiBattery, RuichiCount}

object AlarmStart extends AlarmStreaming{
    def main(args: Array[String]): Unit = {
        val streaming = new AlarmStreaming()
        //核心处理类的实现
        streaming.setBatteryProcessFunction(new RuichiBattery)
        //报警计数类的实现
        streaming.setAlarmCountClass(new RuichiCount)
        streaming.run(args)
    }



}
