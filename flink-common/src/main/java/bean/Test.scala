package bean

import bean.base.ConfigParams
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import streaming.AlarmStreaming.args


object Test {
  def main(args: Array[String]): Unit = {
//    val configParams = ConfigParams.getInstance(ParameterTool.fromArgs(args))
//    val configParams2 = ConfigParams.getInstance(ParameterTool.fromArgs(args))
    //多线程测试，开10个线程，每个线程获取1次实例
    for (i <- 1 to 10) {
      new Thread(new Runnable {
        override def run(): Unit = {
          val configParams = ConfigParams.getInstance(ParameterTool.fromArgs(args))
          println(configParams)
        }
      }).start()
    }


  }

}
