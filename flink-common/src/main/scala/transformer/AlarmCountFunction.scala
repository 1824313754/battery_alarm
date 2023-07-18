package transformer

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


class AlarmCountFunction extends KeyedProcessFunction[String, JSONObject, JSONObject] {
  //定义一个值状态，用来保存历史JSONObject值,类型为JSONObject
  lazy val lastValueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("lastValueState", classOf[JSONObject]))
  var mapCount:Map[String,Int]=_
  var timeInterval=0
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    //获取反射对象
    val base = Class.forName(properties.get("flink.base")).newInstance()
    val fields = base.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    //获取成员变量timeInterval和alarmLevelMap
    timeInterval = fields.filter(_.getName == "timeInterval").head.get(base).asInstanceOf[Int]
    mapCount = fields.filter(_.getName == "mapCount").head.get(base).asInstanceOf[Map[String, Int]]
  }

  override def processElement(cur_alarm: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit =
    {
      //历史值
      val last_alarm: JSONObject = lastValueState.value()
      val counts: Int = mapCount.get(cur_alarm.getString("alarm_type")).getOrElse(1)
      //若当前报警counts=1,则直接输出，并更新状态
      if (last_alarm == null) {
        if (counts == 1) {
          collector.collect(cur_alarm)
      }else{
          //若当前报警counts>1,则更新状态
          lastValueState.update(cur_alarm)
        }
      }
      //否则判断last_alarm的getLong("start_time")和cur_alarm的getLong("start_time")时间差是否小于10s,若满足则getIntValue("alarm_count")加1，直到cur_alarm的getIntValue("alarm_count")等于counts，则输出
        else {
          if (cur_alarm.getLongValue("start_time") - last_alarm.getLongValue("start_time") > 0 && cur_alarm.getLongValue("start_time") - last_alarm.getLongValue("start_time") <= timeInterval) {
            cur_alarm.put("alarm_count", cur_alarm.getIntValue("alarm_count") + last_alarm.getIntValue("alarm_count"))
            if (cur_alarm.getIntValue("alarm_count") == counts) {
              cur_alarm.put("start_time" , last_alarm.getLongValue("start_time"))
              collector.collect(cur_alarm)
              lastValueState.clear()
            } else {
              lastValueState.update(cur_alarm)
            }
          } else {
            lastValueState.update(cur_alarm)
        }
      }
    }

}
