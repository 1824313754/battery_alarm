package streaming

import `enum`.AlarmEnum
import base.BatteryStateFunction
import bean.ClickhouseAlarmTable
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import sink.ClickHouseSink
import source.MyKafkaDeserializationSchema
import transformer.{AlarmCountFunction, AlarmListFlatmap, DataPreprocessing, GpsProcess}
import utils.GetConfig
import utils.GetConfig.createConsumerProperties

import scala.collection.JavaConverters.bufferAsJavaListConverter

object AlarmStreaming extends Serializable with App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //使用Kryo序列化
  env.getConfig.enableForceKryo()
  val tool: ParameterTool = ParameterTool.fromArgs(args)
  val fileName: String = tool.get("config_path")
  val properties = GetConfig.getProperties(fileName)
  //注册为全局变量
  env.getConfig.setGlobalJobParameters(properties)
  //topic
  val topicString = properties.get("kafka.topic")
  //消费者组id
  val groupId = properties.get("kafka.consumer.groupid")
  //获取gps路径
  val gpsPath: String = properties.get("gps.path")
  //注册分布式缓存
  env.registerCachedFile(gpsPath,"gps")
  val topicList: java.util.List[String] = topicString.split(",").toBuffer.asJava
  val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
    topicList,
    new MyKafkaDeserializationSchema(),
    createConsumerProperties(properties)
  )

  val dataStream: DataStream[String] = env.addSource(kafkaConsumer)
  //TODO 将kafka中的json数据转换为JSONObject,去掉枚举中AlarmEnum的key
  val jsonStream: DataStream[JSONObject] = dataStream.map(line=>{
    val jsonObject: JSONObject = JSON.parseObject(line)
    AlarmEnum.values.foreach(enum=>{
      jsonObject.put(enum.toString,0)
    })
    jsonObject
  })

  //TODO 将json数据预处理关联gpsInfo
  val value: DataStream[JSONObject] = jsonStream.map(new DataPreprocessing)
  //  //反射获取配置文件的FlinkBase实现类
  val batteryStateFunction: BatteryStateFunction = Class.forName(properties.get("flink.base")).newInstance().asInstanceOf[BatteryStateFunction]
  val alarmJson: DataStream[JSONObject]=value.keyBy((value: JSONObject) => {
    //根据vin,alarmType,commandType进行分组
    value.getString("vin") + JSON.parseObject(value.getString("customField")).get("commandType")
  }).process(batteryStateFunction)

  //其中一条报警数据可能包含多个报警类型，所以需要将报警数据拆分成多条
  val alarmData: DataStream[JSONObject] = alarmJson.flatMap(new AlarmListFlatmap())
  //对报警数据进行计数统计
  val reslust=alarmData.keyBy((value: JSONObject) => {
    //根据vin,alarmType,commandType进行分组
    value.getString("vin") + value.getString("alarm_type")
  }).process(new AlarmCountFunction).map(new GpsProcess)

  //写入clickhouse
  reslust.addSink(new ClickHouseSink(properties))
  //TODO 报警数据计数统计
//  val alarmCount: DataStream[JSONObject] = alarmData.flatMap(new AlarmCountFlatMap)
//  alarmData.print()
  env.execute("Kafka to console")
}
