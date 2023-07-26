package base

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import sink.ClickHouseSink
import source.MyKafkaDeserializationSchema
import transformer.{AlarmListFlatmap, DataPreprocessing, GpsProcess}
import utils.GetConfig
import utils.GetConfig.createConsumerProperties

import scala.collection.JavaConverters.bufferAsJavaListConverter

trait AlarmStreaming extends FlinkBatteryProcess {

  override def getConfig(args: Array[String]): ParameterTool = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val fileName: String = tool.get("config_path")
    val properties = GetConfig.getProperties(fileName)
    properties
  }

  override def initFlinkEnv(): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceKryo()
    //设置checkpoint
    env.enableCheckpointing(1000)
    //设置重启策略，3次重启，每次间隔5秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    //注册为全局变量
    env.getConfig.setGlobalJobParameters(properties)
    env
  }

  override def registerConfigCachedFile(): Unit = {
    //gps配置文件
    env.registerCachedFile(properties.get("gps.path"), "gps")
    //报警次数配置文件
    env.registerCachedFile(properties.get("alarm.count.path"), "alarmCount")

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
    val dataStream: DataStream[String] = env.addSource(kafkaConsumer)
    dataStream
  }

  override def process(): DataStream[String] = {
    //TODO 将json数据预处理关联gpsInfo
    val value: DataStream[JSONObject] = dataStream.map(new DataPreprocessing)
    val alarmJson: DataStream[JSONObject] = value.keyBy((value: JSONObject) => {
      //根据vin,alarmType,commandType进行分组
      value.getString("vin") + JSON.parseObject(value.getString("customField")).get("commandType")
    }).process(batteryProcessFunction)

    //其中一条报警数据可能包含多个报警类型，所以需要将报警数据拆分成多条
    val alarmData: DataStream[JSONObject] = alarmJson.flatMap(new AlarmListFlatmap())
    //对报警数据进行计数统计
    val reslust: DataStream[String] = alarmData.keyBy((value: JSONObject) => {
      //根据vin,alarmType,commandType进行分组
      value.getString("vin") + value.getString("alarm_type")
    }).process(alarmCountClass).map(new GpsProcess)
    reslust
  }

  override def writeClickHouse(): Unit = {
    //写入clickhouse
    resultStream.addSink(new ClickHouseSink(properties))
  }


}
