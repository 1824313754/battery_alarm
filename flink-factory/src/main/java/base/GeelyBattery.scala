package base

import `enum`.AlarmEnum._
import com.alibaba.fastjson.{JSON, JSONObject}
import model.{ModelFunction_LFP, ModelFunction_NCM}

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
    //加载模型
    val model_LFP = loadModel[ModelFunction_LFP]("model.ModelFunction_LFP")
    val model_NCM = loadModel[ModelFunction_NCM]("model.ModelFunction_NCM")
    //获取模型的方法
    val customField = json.getString("customField")
    val customFieldJson: JSONObject = JSON.parseObject(customField)
    val commandType: Int = customFieldJson.getIntValue("commandType")
    val batteryType = customFieldJson.getIntValue("battery_type")
    val cellCount: Int = json.getIntValue("cellCount")
    json.put("cellVoltagesbackup", json.getString("cellVoltages"))
    json.put("probeTemperaturesbackup", json.getString("probeTemperatures"))
    //TODO 三元
    if(batteryType==2){
      //采集类故障先判定
      model_NCM.isTempAbnormal(json) //温度异常
      model_NCM.isTempLineFall(json) //温感采集线脱落报警
      model_NCM.isVoltageAbnormal(json) //电压异常
      model_NCM.isVoltagelinefFall(json) //电压采集线脱落报警
      model_NCM.isCellVoltageNeighborFault(json) //相邻单体数据采集异常
      model_NCM.isBatteryHighTemperature(json) //高温报警
      model_NCM.isBatteryDiffTemperature(json) //温差报警
      model_NCM.isBatteryUnderVoltage(json) //单体欠压
      model_NCM.isMonomerBatteryOverVoltage(json) //单体过压
      model_NCM.isDynamicDifferential(json) //动态压差
      model_NCM.isDeviceTypeOverVoltage(json) //总压过压
      model_NCM.isDeviceTypeUnderVoltage(json) //总压欠压
      model_NCM.isSocLow(json) //soc过低
      if (old_data != null && commandType == 2) {
        model_NCM.isInsulationAlarm(old_data, json) //绝缘报警
        model_NCM.isSocVirtualHigh(old_data, json,socData52Ah); //soc虚高
        model_NCM.isSocJump(old_data, json); //soc跳变
        model_NCM.isStaticDifferential(old_data, json,socData52Ah) //静态压差
      }
      //TODO 磷酸铁锂
    }else if (batteryType == 1 && cellCount != 1){
      //采集类故障先判定
      model_LFP.isTempAbnormal(json) //温度异常
      model_LFP.isTempLineFall(json) //温感采集线脱落报警
      model_LFP.isVoltageAbnormal(json) //电压异常
      model_LFP.isVoltagelinefFall(json) //电压采集线脱落报警
      model_LFP.isCellVoltageNeighborFault(json) //相邻单体数据采集异常

      model_LFP.isBatteryHighTemperature(json) //高温报警
      model_LFP.isBatteryDiffTemperature(json) //温差报警
      model_LFP.isBatteryUnderVoltage(json) //单体欠压
      model_LFP.isMonomerBatteryOverVoltage(json) //单体过压
      if (json.getInteger("cellCount") == 104) {
        model_LFP.isDynamicDifferential_DJ2136(json) //动态压差DJ2136
      } else if (json.getInteger("cellCount") == 28) {
        model_LFP.isDynamicDifferential_DJ2137(json) //动态压差DJ2137
      }
      model_LFP.isDeviceTypeOverVoltage(json) //总压过压
      model_LFP.isDeviceTypeUnderVoltage(json) //总压欠压
      model_LFP.isSocLow(json) //soc过低
      if (old_data != null && commandType == 2) {
        model_LFP.isInsulationAlarm(old_data, json) //绝缘报警
        model_LFP.isSocJump(old_data, json); //soc跳变
        if (old_data.getIntValue("cellCount") == 104 && json.getIntValue("cellCount") == 104) {
          model_LFP.isStaticDifferential_DJ2136(old_data, json) //静态压差DJ2136
          model_LFP.isSocVirtualHigh_DJ2136(old_data, json,socData2136); //soc虚高DJ2136
        } else if (old_data.getIntValue("cellCount") == 28 && json.getIntValue("cellCount") == 28) {
          model_LFP.isStaticDifferential_DJ2137(old_data, json) //静态压差DJ2137
          model_LFP.isSocVirtualHigh_DJ2137(old_data, json,socData2137); //soc虚高DJ2137
        }
      }
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
}
