package transformer

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichMapFunction
import utils.CommonFuncs.mkctime

class DataPreprocessing extends RichMapFunction[JSONObject,JSONObject]{
  override def map(json: JSONObject): JSONObject = {
    val timeStamp: Long = mkctime(json.getInteger("year")
      , json.getInteger("month")
      , json.getInteger("day")
      , json.getInteger("hours")
      , json.getInteger("minutes")
      , json.getInteger("seconds"))
    json.put("timeStamp", timeStamp)
    json.put("cellVoltagesbackup", json.getString("cellVoltages"))
    json.put("probeTemperaturesbackup", json.getString("probeTemperatures"))
    json
  }
}
