package base

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


trait AlarmCountFunction extends KeyedProcessFunction[String, JSONObject, JSONObject] {
  //定义一个值状态，用来保存历史JSONObject值,类型为JSONObject
  lazy val lastAlarmValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastAlarmValueState", classOf[JSONObject]))


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def processElement(cur_alarm: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
    //历史值
    val last_alarm: JSONObject = lastAlarmValueState.value()
    if (alarmCountFunction(cur_alarm, last_alarm)) {
      collector.collect(cur_alarm)
      lastAlarmValueState.clear()
    } else {
      lastAlarmValueState.update(cur_alarm)
    }
  }

  //报警计数函数
  def alarmCountFunction(cur_alarm: JSONObject, last_alarm: JSONObject):Boolean
}
