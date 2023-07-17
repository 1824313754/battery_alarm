package base

import bean.DictConfig
import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class BatteryStateFunction extends KeyedProcessFunction[String, JSONObject, JSONObject]{
  lazy val lastValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastValueStateLFP", classOf[JSONObject]))
  //定义一个值，用来保存TreeMap[Int,ArrayBuffer[(Int,Float)]]
  var socData: Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]] = _
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties= getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val params = DictConfig.getInstance(properties)
    socData=params.creatInstanceNCM()
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit =
    {
      val json = batteryRuleProcessing(lastValueState.value(), value)
      lastValueState.update(json)
      out.collect(json)
    }

  //定义一个抽象方法，用于处理电池数据
  def batteryRuleProcessing(old_data: JSONObject, new_data: JSONObject): JSONObject


}
