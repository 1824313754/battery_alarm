package base

import `enum`.AlarmEnum._
import com.alibaba.fastjson.{JSON, JSONObject}
import imp.GeelyLFP.{addGeelyLFP1, addGeelyLFP2}
import imp.GeelyNCM
import imp.GeelyNCM.{addGeelyNCM1, addGeelyNCM2}
import org.apache.flink.streaming.api.datastream.DataStream

class GeelyBase extends FlinkBase {
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

  override def process(dataStream: DataStream[JSONObject]) = {
    val GeelyRealData_NCM: DataStream[JSONObject] = filterGeelyData(dataStream, 2, 2)
    val GeelyReissueData_NCM: DataStream[JSONObject] = filterGeelyData(dataStream, 1, 2)
    val GeelyRealData_LFP: DataStream[JSONObject] = filterGeelyData(dataStream, 2, 1, Some(1))
    val GeelyReissueData_LFP: DataStream[JSONObject] = filterGeelyData(dataStream, 1, 1, Some(1))
    // 吉利三元
    val GeelyDataNCM1: DataStream[JSONObject] = addGeelyNCM1(GeelyRealData_NCM)
    val GeelyDataNCM2: DataStream[JSONObject] = addGeelyNCM2(GeelyReissueData_NCM)
    // 吉利铁锂
    val GeelyDataLFP1: DataStream[JSONObject] = addGeelyLFP1(GeelyRealData_LFP)
    val GeelyDataLFP2: DataStream[JSONObject] = addGeelyLFP2(GeelyReissueData_LFP)
    val value = GeelyDataNCM1.union(GeelyDataNCM2, GeelyDataLFP1, GeelyDataLFP2)
    value
  }

  //定义一个方法用于分流
  def filterGeelyData(GeelyVehicldes: DataStream[JSONObject], commandType: Int, batteryType: Int, cellCount: Option[Int] = None): DataStream[JSONObject] = {
    GeelyVehicldes.filter(json => {
      val customField = json.getString("customField")
      val customFieldJson: JSONObject = JSON.parseObject(customField)
      val cmdType: Int = customFieldJson.getIntValue("commandType")
      val batType = customFieldJson.getIntValue("battery_type")
      val cCount = json.getIntValue("cellCount")
      cmdType == commandType && batType == batteryType && cellCount.forall(_ != cCount)
    })
  }

}
