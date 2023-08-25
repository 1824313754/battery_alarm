package base

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import sink.ClickHouseSink
import source.MyKafkaDeserializationSchema
import transformer.{AlarmListFlatmap, DataPreprocessing, GpsProcess}
import utils.GetConfig
import utils.GetConfig.createConsumerProperties

import scala.collection.JavaConverters.bufferAsJavaListConverter

class AlarmStreaming extends FlinkBatteryProcess {
  override def getConfig(args: Array[String]): ParameterTool = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val fileName: String = tool.get("config_path")
    val properties = GetConfig.getProperties(fileName)
    properties
  }

  override def initFlinkEnv(): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceKryo()
    //获取当前环境
    properties.get("flink.env") match {
      case "test" => env.setParallelism(1)
      case _ =>
        //设置checkpoint
        env.enableCheckpointing(properties.get("checkpoint.interval").toInt)
        //设置重启策略，3次重启，每次间隔5秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(properties.getInt("restart.num"), properties.getLong("restart.interval")))
        //设置最大checkpoint并行度
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        //设置checkpoint超时时间
        env.getCheckpointConfig.setCheckpointTimeout(properties.getLong("checkpoint.timeout"))
        //设置checkpoint失败时任务是否继续运行
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(properties.getInt("checkpoint.failure.num"))
        //设置HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend())
        //设置checkpoint目录
        env.getCheckpointConfig.setCheckpointStorage(properties.get("checkpoint.path"))
        //设置任务取消时保留checkpoint
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }
    //注册为全局变量
    env.getConfig.setGlobalJobParameters(properties)
    env
  }

  override def registerConfigCachedFile(): Unit = {
    //gps配置文件
    env.registerCachedFile(properties.get("gps.path"), "gps")


  }

  override def readKafka() = {
    val topicString = properties.get("kafka.topic")
    //消费者组id
    val groupId = properties.get("kafka.consumer.groupid")
    val topicList: java.util.List[String] = topicString.split(",").toBuffer.asJava
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
      topicList,
      new MyKafkaDeserializationSchema(groupId),
      createConsumerProperties(properties)
    )
    val dataStream: DataStream[String] = env.addSource(kafkaConsumer).uid("kafkaSource").name("kafkaSource")
    dataStream
  }

  override def process(): DataStream[String] = {
    //TODO 将json数据预处理
    val value: DataStream[JSONObject] = dataStream.map(new DataPreprocessing).uid("dataPreprocessing").name("dataPreprocessing")
    val alarmJson: DataStream[JSONObject] = value.keyBy(new KeySelector[JSONObject,String] {
      //根据vin,alarmType,commandType进行分组
      override def getKey(in: JSONObject): String = in.getString("vin") + JSON.parseObject(in.getString("customField")).get("commandType")
    }).process(batteryProcessFunction).uid("batteryProcessFunction").name("batteryProcessFunction")

    //其中一条报警数据可能包含多个报警类型，所以需要将报警数据拆分成多条
    val alarmData: DataStream[JSONObject] = alarmJson.flatMap(new AlarmListFlatmap()).uid("alarmListFlatmap").name("alarmListFlatmap")
    //对报警数据进行计数统计
    val reslust: DataStream[String] = alarmData.keyBy(new KeySelector[JSONObject,String] {
    //根据vin,alarmType,commandType进行分组
      override def getKey(in: JSONObject): String = in.getString("vin") + in.getString("alarm_type")
    }).process(alarmCountClass).uid("alarmCountClass").name("alarmCountClass")
      .map(new GpsProcess).uid("gpsProcess").name("gpsProcess")
    reslust
  }

  override def writeClickHouse(): Unit = {
    //写入clickhouse
    resultStream.addSink(new ClickHouseSink(properties)).uid("clickHouseSink").name("clickHouseSink")
  }


}
