package geely

import base.FlinkBatteryProcess
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object GeelyAlarm extends FlinkBatteryProcess with App {
  override def getJobName(): String = "GeelyAlarm"
  run(args)
}
