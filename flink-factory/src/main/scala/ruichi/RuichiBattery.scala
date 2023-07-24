package ruichi


import `enum`.AlarmEnum._
import base.BatteryStateFunction
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RuichiBattery extends BatteryStateFunction{
  //定义车厂2帧的数据时间间隔(s)
  val timeInterval = 15
  //设置报警对应的报警次数
  val mapCount: Map[String, Int] = Map(
    batteryHighTemperature.toString -> 3,
    socJump.toString -> 1,
    socHigh.toString -> 1,
    monomerBatteryUnderVoltage.toString -> 3,
    monomerBatteryOverVoltage.toString -> 3,
    deviceTypeUnderVoltage.toString -> 3,
    deviceTypeOverVoltage.toString -> 3,
    batteryConsistencyPoor.toString -> 3,
    insulation.toString -> 1,
    socLow.toString -> 3,
    temperatureDifferential.toString -> 3,
    voltageJump.toString -> 3,
    socNotBalance.toString -> 3,
    electricBoxWithWater.toString -> 3,
    outFactorySafetyInspection.toString -> 3,
    abnormalTemperature.toString -> 8,
    abnormalVoltage.toString -> 8,
    voltageLineFall.toString -> 8,
    tempLineFall.toString -> 8,
    slaveDisconnect.toString -> 8,
    isAdjacentMonomerAbnormal.toString -> 3,
    batteryStaticConsistencyPoor.toString -> 1,
    isAbnormalinternalResistance.toString -> 3
  )
  //这里必须加上lazy，否则会报错，因为socData还没有初始化,batteryRuleProcessing方法中会用到
  lazy val socDataRC: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]] =socData.getOrElse("RuiChi-63AH",null)
  override def batteryRuleProcessing(old_data: JSONObject, new_data: JSONObject): JSONObject = {
    val customField = new_data.getString("customField")
    val customFieldJson: JSONObject = JSON.parseObject(customField)
    val commandType: Int = customFieldJson.getIntValue("commandType")
    //采集类故障先判定
    RC.isSlavedisconnect(new_data) // 从机掉线
    RC.isTempAbnormal(new_data) //温度异常
    RC.isTempLineFall(new_data) //温感采集线脱落报警
    RC.isVoltageAbnormal(new_data) //电压异常
    RC.isVoltagelinefFall(new_data) //电压采集线脱落报警
    RC.isCellVoltageNeighborFault(new_data) //相邻单体数据采集异常

    RC.isBatteryHighTemperature(new_data) //高温报警
    RC.isBatteryDiffTemperature(new_data) //温差报警
    RC.isBatteryUnderVoltage(new_data) //单体欠压
    RC.isMonomerBatteryOverVoltage(new_data) //单体过压
    RC.isDynamicDifferential(new_data) //动态压差
    RC.isDeviceTypeOverVoltage(new_data) //总压过压
    RC.isDeviceTypeUnderVoltage(new_data) //总压欠压
    RC.isSocLow(new_data) //soc过低
    if (old_data != null && commandType == 2) {
      RC.isInsulationAlarm(old_data, new_data) //绝缘报警
      RC.isSocVirtualHigh(old_data,new_data,socDataRC); //soc虚高
      RC.isSocJump(old_data, new_data); //soc跳变
      RC.isStaticDifferential(old_data, new_data) //静态压差
    }
    new_data
  }

  override def getRediesKey(): String = "RuiChi_004:"
}
