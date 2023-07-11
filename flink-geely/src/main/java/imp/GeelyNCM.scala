package imp

import com.alibaba.fastjson.{JSON, JSONObject}
import model.ModelFunction_NCM._
import org.apache.flink.streaming.api.datastream.DataStream
import state.GeelyNCMState

object GeelyNCM extends Serializable {
  def addGeelyNCM1(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    println("Geely NCM_RealData is comeing")
    val GeelyVinJson: DataStream[JSONObject] = addGeelyAlarm(persistsParts)
    val value = GeelyVinJson.keyBy((value: JSONObject) => value.getString("vin")).process(new GeelyNCMState)
    value
  }

  def addGeelyNCM2(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    println("Geely NCM_ReissueData is comeing")
    val GeelyVinJson: DataStream[JSONObject] = addGeelyAlarm(persistsParts)
    GeelyVinJson
  }

  def addGeelyAlarm(persistsParts: DataStream[JSONObject]): DataStream[JSONObject] = {
    val value: DataStream[JSONObject] = persistsParts.map {
      json => {
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
        isDynamicDifferential(json) //动态压差
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
