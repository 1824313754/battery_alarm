package base

import bean.DictConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

trait BatteryStateFunction extends KeyedProcessFunction[String, JSONObject, JSONObject] {

  lazy val lastValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastValueStateLFP", classOf[JSONObject]))
  //定义一个值，用来保存TreeMap[Int,ArrayBuffer[(Int,Float)]]
  var socData: Map[String, TreeMap[Int, ArrayBuffer[(Int, Float)]]] = _


  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties: ParameterTool = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val params = DictConfig.getInstance(properties)
    socData=params.creatInstanceNCM()
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit =
    {
      val last_json=lastValueState.value()
      val json = batteryRuleProcessing(last_json, value)
      lastValueState.update(json)
      out.collect(json)
    }

  //报警核心处理函数
  def batteryRuleProcessing(last_json:JSONObject,value:JSONObject):JSONObject

}
