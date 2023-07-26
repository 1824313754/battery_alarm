package transformer

import `enum`.AlarmEnum
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichMapFunction
import utils.CommonFuncs.mkctime

class DataPreprocessing extends RichMapFunction[String,JSONObject]{
  override def map(str: String): JSONObject = {
    val json = JSON.parseObject(str)
    val timeStamp: Long = mkctime(json.getInteger("year")
      , json.getInteger("month")
      , json.getInteger("day")
      , json.getInteger("hours")
      , json.getInteger("minutes")
      , json.getInteger("seconds"))
    json.put("timeStamp", timeStamp)
    json.put("cellVoltagesbackup", json.getString("cellVoltages"))
    json.put("probeTemperaturesbackup", json.getString("probeTemperatures"))
    //若json中包含了枚举类中的报警类型，则赋值为0
    for (alarmType <- AlarmEnum.values) {
      if (json.keySet.contains(alarmType.toString)) {
        json.put(alarmType.toString,0)
      }
    }
    json
  }
}
