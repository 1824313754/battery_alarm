import base.{FlinkBatteryProcess}

object AlarmStart extends FlinkBatteryProcess with App {
  override def getJobName(): String = properties.get("flink.base")
  run(args)
}
