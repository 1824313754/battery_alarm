package state

import bean.base.ConfigParams
import com.alibaba.fastjson.{JSON, JSONObject}
import model.ModelFunction_LFP.{isSocVirtualHigh_DJ2136, isSocVirtualHigh_DJ2137, isStaticDifferential_DJ2136, isStaticDifferential_DJ2137}
import model.ModelFunction_NCM._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GeelyLFPState extends KeyedProcessFunction[String,JSONObject,JSONObject]{
  lazy val lastValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastValueStateLFP", classOf[JSONObject]))
  //定义一个值，用来保存TreeMap[Int,ArrayBuffer[(Int,Float)]]
  var socData2137:mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]] =_
  var socData2136:mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]] =_
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties= getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val params = ConfigParams.getInstance(properties)
    socData2137=params.creatInstanceNCM().getOrElse("Geely-DJ2137",socData2137)
    socData2136=params.creatInstanceNCM().getOrElse("Geely-DJ2136",socData2136)
  }

  override def processElement(value: JSONObject, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val old_data: JSONObject = lastValueState.value()
    val new_data: JSONObject = value
    if (old_data != null) {
      isInsulationAlarm(old_data, new_data) //绝缘报警
      isSocJump(old_data, new_data); //soc跳变
      if (old_data.getIntValue("cellCount") == 104 && new_data.getIntValue("cellCount") == 104) {
        isStaticDifferential_DJ2136(old_data, new_data) //静态压差DJ2136
        isSocVirtualHigh_DJ2136(old_data, new_data,socData2136); //soc虚高DJ2136
      } else if (old_data.getIntValue("cellCount") == 28 && new_data.getIntValue("cellCount") == 28) {
        isStaticDifferential_DJ2137(old_data, new_data) //静态压差DJ2137
        isSocVirtualHigh_DJ2137(old_data, new_data,socData2137); //soc虚高DJ2137
      }

      new_data.put("cellVoltages", new_data.getString("cellVoltagesbackup"))
      new_data.remove("cellVoltagesbackup")
      new_data.put("probeTemperatures", new_data.getString("probeTemperaturesbackup"))
      new_data.remove("probeTemperaturesbackup")
      val dcstatus: Integer = new_data.getInteger("dcStatus")
      val timeStamp: Long = new_data.getLong("timeStamp")
      if (old_data == null && dcstatus == 0) {
        new_data.put("DCTime", timeStamp)
      }
      lastValueState.update(new_data)
      //    RedisUtil.setKey(PREFIX_KEY + vin,jsonStr,0)
      out.collect(new_data)
    }
  }
}
