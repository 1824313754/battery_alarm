package base

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment}
import streaming.AlarmStreaming.batteryProcess
import utils.GetConfig

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit

trait FlinkBatteryProcess {
  protected var env: StreamExecutionEnvironment = _
  protected var properties: ParameterTool = _
  def init(args: Array[String]): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    //使用Kryo序列化
    env.getConfig.enableForceKryo()
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val fileName: String = tool.get("config_path")
    properties = GetConfig.getProperties(fileName)
    //注册为全局变量
    env.getConfig.setGlobalJobParameters(properties)
    val restartStrategy = properties.get("restartStrategy")
    if ("fixedDelayRestart" == restartStrategy) env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(60, TimeUnit.SECONDS)))
    else if ("noRestart" == restartStrategy) env.setRestartStrategy(RestartStrategies.noRestart)
    else if ("fallBackRestart" == restartStrategy) env.setRestartStrategy(RestartStrategies.fallBackRestart)
    else env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(60, TimeUnit.SECONDS)))
//    val isLocal = properties.get("isLocal")
//    if (StringUtils.isBlank(isLocal)) {
//      val isIncremental = properties.get("isIncremental")
//      Preconditions.checkNotNull(isIncremental, "isIncremental is null")
//      var stateBackend:RocksDBStateBackend = null
//      val hadoopIp = properties.get("hadoopIp")
//      if ("isIncremental" == isIncremental) { //如果本地调试，必须指定hdfs的端口信息，且要依赖hadoop包，如果集群执行，flink与hdfs在同一集群，那么可以不指定hdfs端口信息，也不用将hadoop打进jar包。
//        stateBackend = new RocksDBStateBackend("hdfs:///home/intsmaze/flink/" + getJobName, true)
//        env.setStateBackend(stateBackend)
//      }
//      else if ("full" == isIncremental) {
//        stateBackend = new RocksDBStateBackend("hdfs://" + hadoopIp + "/home/intsmaze/flink/" + getJobName, false)
//        env.setStateBackend(stateBackend)
//      }
//      env.enableCheckpointing(5000)
//      env.getCheckpointConfig.setCheckpointTimeout(30000)
//      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
//      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//      env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//      env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    }
  }

  def getJobName():String

  def run(args: Array[String]): Unit = {
    init(args)
    batteryProcess(properties,env)
    //当前时间转为yyyy-MM-dd HH:mm:ss格式
    val topoName = StringUtils.join(getJobName, "-", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
    env.execute(topoName)
  }

}
