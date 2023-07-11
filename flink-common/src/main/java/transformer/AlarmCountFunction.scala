package transformer

import bean.ClickhouseAlarmTable
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


class AlarmCountFunction extends KeyedProcessFunction[String, ClickhouseAlarmTable, ClickhouseAlarmTable] {
  //定义一个值状态，用来保存历史ClickhouseAlarmTable值,类型为ClickhouseAlarmTable
  lazy val lastValueState: ValueState[ClickhouseAlarmTable] = getRuntimeContext.getState(new ValueStateDescriptor[ClickhouseAlarmTable]("lastValueState", classOf[ClickhouseAlarmTable]))
  var mapCount:Map[String,Int]=_
 var  timeInterval=0
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

  override def processElement(cur_alarm: ClickhouseAlarmTable, context: KeyedProcessFunction[String, ClickhouseAlarmTable, ClickhouseAlarmTable]#Context, collector: Collector[ClickhouseAlarmTable]): Unit =
    {
      //历史值
      val last_alarm: ClickhouseAlarmTable = lastValueState.value()
      val counts: Int = mapCount.get(cur_alarm.alarm_type).getOrElse(1)
      //若当前报警counts=1,则直接输出，并更新状态
      if (last_alarm == null) {
        if (counts == 1) {
          collector.collect(cur_alarm)
      }else{
          //若当前报警counts>1,则更新状态
          lastValueState.update(cur_alarm)
        }
      }
      //否则判断last_alarm的start_time和cur_alarm的start_time时间差是否小于10s,若满足则alarm_count加1，直到cur_alarm的alarm_count等于counts，则输出
        else {
          if (cur_alarm.start_time - last_alarm.start_time > 0 && cur_alarm.start_time - last_alarm.start_time <= timeInterval) {
            cur_alarm.alarm_count = cur_alarm.alarm_count + last_alarm.alarm_count
            if (cur_alarm.alarm_count == counts) {
              cur_alarm.start_time = last_alarm.start_time
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
