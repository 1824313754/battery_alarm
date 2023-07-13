package base

import bean.base.ConfigParams
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
    val params = ConfigParams.getInstance(properties)
    socData=params.creatInstanceNCM()
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit =
    {
      val json = batteryRuleProcessing(lastValueState.value(), value)
      lastValueState.update(json)
      out.collect(json)
    }

  def batteryRuleProcessing(old_data: JSONObject, new_data: JSONObject): JSONObject


  //定义方法加载模型,使用反射机制
  def loadModel[T: ClassTag](modelName: String): T = {
    val modelClass = Class.forName(modelName)
    val model = modelClass.newInstance()
    model.asInstanceOf[T]
  }
}
