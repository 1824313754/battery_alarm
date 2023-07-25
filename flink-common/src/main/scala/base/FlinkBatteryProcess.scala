package base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import utils.CommonFuncs.timestampToDate

import scala.beans.BeanProperty

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




  //获取配置文件
  def getConfig(args: Array[String]): ParameterTool

  //初始化flink环境
  def initFlinkEnv(): StreamExecutionEnvironment

  //注册一些连接信息为分布式缓存
  def registerConfigCachedFile()

  //读取kafka数据
  def readKafka():DataStream[String]

  //处理数据
  def process(batteryStateFunction: BatteryStateFunction):DataStream[String]

  //写入clickhouse
  def writeClickHouse()



  def run(args: Array[String]): Unit = {
    this.properties=getConfig(args)
    this.env=initFlinkEnv()
    //注册一些连接信息为分布式缓存
    registerConfigCachedFile()
    //读取kafka数据
    this.dataStream=readKafka()
    //处理数据的核心类,反射加载
    val batteryStateFunction: BatteryStateFunction = Class.forName(properties.get("flink.base")).newInstance().asInstanceOf[BatteryStateFunction]

    this.resultStream=process(batteryStateFunction)
    //写入clickhouse
    writeClickHouse()
    env.execute("flink-battery-alarm")
  }

}
