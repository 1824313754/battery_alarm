package imp

import com.alibaba.fastjson.{JSON, JSONObject}
import model.ModelFunction_LFP._
import org.apache.flink.streaming.api.datastream.DataStream
import state.{GeelyLFPState}
import utils.CommonFuncs.mkctime

object GeelyLFP {

  def addGeelyLFP1(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    println("Geely LFP_RealData is comeing")
    val GeelyVinJson: DataStream[JSONObject] = addGeelyAlarm(persistsParts)
    val value = GeelyVinJson.keyBy((value: JSONObject) => value.getString("vin")).process(new GeelyLFPState)
    value
  }

  def addGeelyLFP2(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    println("Geely LFP_ReissueData is comeing")
    val GeelyVinJson: DataStream[JSONObject] = addGeelyAlarm(persistsParts)
    GeelyVinJson
  }

  def addGeelyAlarm(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    val value: DataStream[JSONObject] = persistsParts.map {
      json => {
        val timeStamp: Long = mkctime(json.getInteger("year")
          , json.getInteger("month")
          , json.getInteger("day")
          , json.getInteger("hours")
          , json.getInteger("minutes")
          , json.getInteger("seconds"))
        json.put("timeStamp", timeStamp)

        json.put("cellVoltagesbackup", json.getString("cellVoltages"))
        json.put("probeTemperaturesbackup", json.getString("probeTemperatures"))

        //采集类故障先判定
        isTempAbnormal(json) //温度异常
        isTempLineFall(json) //温感采集线脱落报警
        isVoltageAbnormal(json) //电压异常
        isVoltagelinefFall(json) //电压采集线脱落报警
        isCellVoltageNeighborFault(json) //相邻单体数据采集异常

        isBatteryHighTemperature(json) //高温报警
        isBatteryDiffTemperature(json) //温差报警
        isBatteryUnderVoltage(json) //单体欠压
        isMonomerBatteryOverVoltage(json) //单体过压
        if (json.getInteger("cellCount") == 104) {
          isDynamicDifferential_DJ2136(json) //动态压差DJ2136
        } else if (json.getInteger("cellCount") == 28) {
          isDynamicDifferential_DJ2137(json) //动态压差DJ2137
        }
        isDeviceTypeOverVoltage(json) //总压过压
        isDeviceTypeUnderVoltage(json) //总压欠压
        isSocLow(json) //soc过低

        if (JSON.parseObject(json.getString("customField")).get("commandType") == 3) {
          json.put("cellVoltages", json.getString("cellVoltagesbackup"))
          json.remove("cellVoltagesbackup")
          json.put("probeTemperatures", json.getString("probeTemperaturesbackup"))
          json.remove("probeTemperaturesbackup")
        }
        json
      }
    }
    value
  }

}
