package base

import bean.DictConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import utils.RedisUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait BatteryStateFunction extends KeyedProcessFunction[String, JSONObject, JSONObject]{
  lazy val lastValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastValueStateLFP", classOf[JSONObject]))
  //定义一个值，用来保存TreeMap[Int,ArrayBuffer[(Int,Float)]]
  var socData: Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]] = _
  //redis实例
  var redis: RedisUtil = _
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties= getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val params = DictConfig.getInstance(properties)
    socData=params.creatInstanceNCM()
    redis=RedisUtil.getInstance(properties)
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit =
    {
      //获取当前vin
      val vin = value.getString("vin")
      var last_json=lastValueState.value()
      if (last_json == null) {
       //从redis获取
        last_json=JSON.parseObject(redis.getKey(getRediesKey+vin,10))
      }
      val json = batteryRuleProcessing(last_json, value)
      //保存到redis
      redis.setKey(getRediesKey+vin,json.toJSONString,10)
      lastValueState.update(json)
      out.collect(json)
    }

  //定义一个抽象方法，用于处理电池数据
  def batteryRuleProcessing(old_data: JSONObject, new_data: JSONObject): JSONObject

  def getRediesKey(): String
}
