package geely

import `enum`.AlarmEnum._
import base.BatteryStateFunction
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GeelyBattery extends BatteryStateFunction{
  //定义车厂2帧的数据时间间隔(s)
  val timeInterval = 10
  //设置报警对应的报警次数
  val mapCount: Map[String, Int] = Map(
    batteryHighTemperature.toString -> 1,
    socJump.toString -> 1,
    socHigh.toString -> 1,
    monomerBatteryUnderVoltage.toString -> 1,
    monomerBatteryOverVoltage.toString -> 1,
    deviceTypeUnderVoltage.toString -> 1,
    deviceTypeOverVoltage.toString -> 1,
    batteryConsistencyPoor.toString -> 1,
    insulation.toString -> 1,
    socLow.toString -> 1,
    temperatureDifferential.toString -> 1,
    voltageJump.toString -> 1,
    socNotBalance.toString -> 1,
    electricBoxWithWater.toString -> 1,
    outFactorySafetyInspection.toString -> 1,
    abnormalTemperature.toString -> 1,
    abnormalVoltage.toString -> 1,
    voltageLineFall.toString -> 1,
    tempLineFall.toString -> 1,
    isAdjacentMonomerAbnormal.toString -> 1,
    batteryStaticConsistencyPoor.toString -> 1,
    isAbnormalinternalResistance.toString -> 1
  )
  //这里必须加上lazy，否则会报错，因为socData还没有初始化,batteryRuleProcessing方法中会用到
  lazy val socData2137: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]] =socData.getOrElse("Geely-DJ2137",null)
  lazy val socData2136: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]=socData.getOrElse("Geely-DJ2136",null)
  lazy val socData52Ah: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]=socData.getOrElse("Geely-NCM-52Ah",null)


  override def batteryRuleProcessing(old_data: JSONObject, json: JSONObject): JSONObject = {
    val customField = json.getString("customField")
    val customFieldJson: JSONObject = JSON.parseObject(customField)
    val commandType: Int = customFieldJson.getIntValue("commandType")
    val batteryType = customFieldJson.getIntValue("battery_type")
    val cellCount: Int = json.getIntValue("cellCount")
    json.put("cellVoltagesbackup", json.getString("cellVoltages"))
    json.put("probeTemperaturesbackup", json.getString("probeTemperatures"))

    //TODO 三元
    if (batteryType == 2) {
      processNCMData(json, old_data, commandType)

    }
    //TODO 磷酸铁锂
    else if (batteryType == 1 && cellCount != 1) {
      processLFPData(json, old_data, commandType)
    }

    json.put("cellVoltages", json.getString("cellVoltagesbackup"))
    json.remove("cellVoltagesbackup")
    json.put("probeTemperatures", json.getString("probeTemperaturesbackup"))
    json.remove("probeTemperaturesbackup")

    val dcstatus: Integer = json.getInteger("dcStatus")
    val timeStamp: Long = json.getLong("timeStamp")
    if (old_data == null && dcstatus == 0) {
      json.put("DCTime", timeStamp)
    }
    json
  }

  private def processNCMData(json: JSONObject, old_data: JSONObject, commandType: Int): Unit = {
    ModelFunction_NCM.isTempAbnormal(json)
    ModelFunction_NCM.isTempLineFall(json)
    ModelFunction_NCM.isVoltageAbnormal(json)
    ModelFunction_NCM.isVoltagelinefFall(json)
    ModelFunction_NCM.isCellVoltageNeighborFault(json)
    ModelFunction_NCM.isBatteryHighTemperature(json)
    ModelFunction_NCM.isBatteryDiffTemperature(json)
    ModelFunction_NCM.isBatteryUnderVoltage(json)
    ModelFunction_NCM.isMonomerBatteryOverVoltage(json)
    ModelFunction_NCM.isDynamicDifferential(json)
    ModelFunction_NCM.isDeviceTypeOverVoltage(json)
    ModelFunction_NCM.isDeviceTypeUnderVoltage(json)
    ModelFunction_NCM.isSocLow(json)
    if (old_data != null && commandType == 2) {
      ModelFunction_NCM.isInsulationAlarm(old_data, json)
      ModelFunction_NCM.isSocVirtualHigh(old_data, json, socData52Ah)
      ModelFunction_NCM.isSocJump(old_data, json)
      ModelFunction_NCM.isStaticDifferential(old_data, json, socData52Ah)
    }
  }

  private def processLFPData(json: JSONObject, old_data: JSONObject, commandType: Int): Unit = {
    ModelFunction_LFP.isTempAbnormal(json)
    ModelFunction_LFP.isTempLineFall(json)
    ModelFunction_LFP.isVoltageAbnormal(json)
    ModelFunction_LFP.isVoltagelinefFall(json)
    ModelFunction_LFP.isCellVoltageNeighborFault(json)
    ModelFunction_LFP.isBatteryHighTemperature(json)
    ModelFunction_LFP.isBatteryDiffTemperature(json)
    ModelFunction_LFP.isBatteryUnderVoltage(json)
    ModelFunction_LFP.isMonomerBatteryOverVoltage(json)

    if (json.getInteger("cellCount") == 104) {
      ModelFunction_LFP.isDynamicDifferential_DJ2136(json)
    } else if (json.getInteger("cellCount") == 28) {
      ModelFunction_LFP.isDynamicDifferential_DJ2137(json)
    }
    ModelFunction_LFP.isDeviceTypeOverVoltage(json)
    ModelFunction_LFP.isDeviceTypeUnderVoltage(json)
    ModelFunction_LFP.isSocLow(json)

    if (old_data != null && commandType == 2) {
      ModelFunction_LFP.isInsulationAlarm(old_data, json)
      ModelFunction_LFP.isSocJump(old_data, json)
      if (old_data.getIntValue("cellCount") == 104 && json.getIntValue("cellCount") == 104) {
        ModelFunction_LFP.isStaticDifferential_DJ2136(old_data, json)
        ModelFunction_LFP.isSocVirtualHigh_DJ2136(old_data, json, socData2136)
      } else if (old_data.getIntValue("cellCount") == 28 && json.getIntValue("cellCount") == 28) {
        ModelFunction_LFP.isStaticDifferential_DJ2137(old_data, json)
        ModelFunction_LFP.isSocVirtualHigh_DJ2137(old_data, json, socData2137)
      }
    }
  }

  override  def getRediesKey(): String = "Geely-005"
}
