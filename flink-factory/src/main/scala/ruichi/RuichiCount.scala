package ruichi

import `enum`.AlarmEnum
import base.AlarmCountFunction
import com.alibaba.fastjson.JSONObject

/**
 * 瑞驰报警计数
 * 返回true，说明需要报警
 * 返回false，说明不需要报警
 */
class RuichiCount extends AlarmCountFunction{
  override def alarmCountFunction(cur_alarm: JSONObject, last_alarm: JSONObject): Boolean = {

    //若为null，说明是第一次报警，直接返回true
    if (last_alarm == null) {
      AlarmEnum.withName(cur_alarm.getString("alarm_type")) match {
        case AlarmEnum.socJump => true
        case AlarmEnum.socHigh => true
        case AlarmEnum.insulation => true
        case AlarmEnum.batteryStaticConsistencyPoor => true
        case _ => false
      }
    } else {
      //判断采集间隔最大为3个采集点
      if (cur_alarm.getLongValue("start_time") - last_alarm.getLong("start_time") <= 15 && cur_alarm.getLong("start_time") - last_alarm.getLong("start_time") > 0) {
        cur_alarm.put("alarm_count",last_alarm.getIntValue("alarm_count") + 1)
        cur_alarm.put("start_time", last_alarm.getLongValue("start_time"))
        if (cur_alarm.getString("alarm_type") == "abnormalVoltage"
          || cur_alarm.getString("alarm_type") == "abnormalTemperature"
          || cur_alarm.getString("alarm_type") == "voltageLineFall"
          || cur_alarm.getString("alarm_type") == "tempLineFall"
          || cur_alarm.getString("alarm_type") == "slaveDisconnect") {
          if (cur_alarm.getIntValue("alarm_count") >= 8 && cur_alarm.getLong("start_time") - last_alarm.getLong("start_time") > 10) {
            true
          } else {
            false
          }
        } else {
          if (cur_alarm.getIntValue("alarm_count") == 3) {
            true
          } else {
            false
          }
        }
      } else {
        false
      }
    }
  }
}
