package base

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


/**
 * @ClassName: FlinkBatteryProcess
 * @Description: TODO flink处理数据的基类
 */

trait FlinkBatteryProcess extends Serializable {
  //flink环境
  protected var env: StreamExecutionEnvironment = _
  //配置文件
  protected var properties: ParameterTool = _
  //数据流
  protected var dataStream: DataStream[String] = _
  //结果数据流
  protected var resultStream: DataStream[String] = _
  //处理数据的核心类
  protected var batteryProcessFunction : BatteryStateFunction=_
  //获取报警次数的类文件
  protected var alarmCountClass:AlarmCountFunction=_


  //获取配置文件
  def getConfig(args: Array[String]): ParameterTool

  //初始化flink环境
  def initFlinkEnv(): StreamExecutionEnvironment

  //注册一些连接信息为分布式缓存
  def registerConfigCachedFile()

  //读取kafka数据
  def readKafka():DataStream[String]

  //处理数据
  def process():DataStream[String]

  //写入clickhouse
  def writeClickHouse()



  def run(args: Array[String]): Unit = {
    this.properties=getConfig(args)
    this.env=initFlinkEnv()
    //注册一些连接信息为分布式缓存
    registerConfigCachedFile()
    //处理数据的核心类,反射加载
    this.batteryProcessFunction = Class.forName(properties.get("flink.base")).newInstance().asInstanceOf[BatteryStateFunction]
    //获取报警次数的类文件
    this.alarmCountClass = Class.forName(properties.get("alarm.count")).newInstance().asInstanceOf[AlarmCountFunction]
    //读取kafka数据
    this.dataStream=readKafka()
    this.resultStream=process()
    //写入clickhouse
    writeClickHouse()
    env.execute("flink-battery-alarm")
  }

}
