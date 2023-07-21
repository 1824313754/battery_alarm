import base.{FlinkBatteryProcess}

object AlarmStart extends FlinkBatteryProcess  {
  def main(args: Array[String]): Unit = {

    run(args)
  }

  override def getJobName(): String = "run AlarmStart"
}
