package ruichi

import `enum`.AlarmEnum
import com.alibaba.fastjson.{JSON, JSONObject}
import coom.AdvancedFuncs.notSocLayerLFP
import utils.MathFuncs.calcSoc

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

object RC extends Serializable {
  /**
   * 高温报警
   *
   * @param json
   */
  def isBatteryHighTemperature(json: JSONObject) {

    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures")) //温感数组
    var maxTemperature: Integer = json.getInteger("maxTemperature")
    //过滤
    if (probeTemperaturesArray != null) {
      //取值判定
      if (probeTemperaturesArray.nonEmpty) {
        maxTemperature = probeTemperaturesArray.max - 40 //再取最大值

        if (maxTemperature >= 60 && maxTemperature < 125) {
          json.put(AlarmEnum.batteryHighTemperature.toString, 3)
        } else if (maxTemperature >= 57 && maxTemperature < 60) {
          json.put(AlarmEnum.batteryHighTemperature.toString, 2)
        } else if (maxTemperature >= 54 && maxTemperature < 57) {
          json.put(AlarmEnum.batteryHighTemperature.toString, 1)
        }
      }
    }
  }

  /**
   * 单体过压
   *
   * @param json
   */
  def isMonomerBatteryOverVoltage(json: JSONObject) {

    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        if ((cellVoltagesArray.min > 3600 && (cellVoltagesArray.max - cellVoltagesArray.min < (150 + (cellVoltagesArray.min - 3600) * 0.5)))
          || (cellVoltagesArray.length != 0 && cellVoltagesArray.sum / cellVoltagesArray.length > 3650))
          return
        if (maxCellVoltage > 3800 && maxCellVoltage < 4200) {
          json.put(AlarmEnum.monomerBatteryOverVoltage.toString, 3)
        } else if (maxCellVoltage > 3750 && maxCellVoltage <= 3800) {
          json.put(AlarmEnum.monomerBatteryOverVoltage.toString, 2)
        } else if (maxCellVoltage > 3700 && maxCellVoltage <= 3750) {
          json.put(AlarmEnum.monomerBatteryOverVoltage.toString, 1)
        }
      }
    }
  }

  /**
   * 单体欠压
   *
   * @param json
   */
  def isBatteryUnderVoltage(json: JSONObject) {
    var minTemperature: Integer = json.getInteger("minTemperature")
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      //取值判定
      if (probeTemperaturesArray.nonEmpty && cellVoltagesArray.nonEmpty) {
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        minCellVoltage = cellVoltagesArray.min //再取最小值

        if (minTemperature > 0 && minTemperature < 125) { //只有都为0V的时候才能跳过前面的过滤，所以剔除
          if (minCellVoltage <= 2200
            && cellVoltagesArray.filter(x => (cellVoltagesArray.sum / cellVoltagesArray.length - x) > 3 * Math.sqrt(cellVoltagesArray.map(x => Math.pow(x - cellVoltagesArray.sum / cellVoltagesArray.length, 2)).sum / cellVoltagesArray.length)).length > 0) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 3) //monomerBatteryUnderVoltage
          } else if (minCellVoltage <= 2350) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 2) //deviceTypeUnderVoltage单体设备欠压
          } else if (minCellVoltage <= 2500) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 1)
          }
        } else if (minTemperature <= 0 && minTemperature > -40) {
          if (minCellVoltage <= 2000
            && cellVoltagesArray.filter(x => (cellVoltagesArray.sum / cellVoltagesArray.length - x) > 3 * Math.sqrt(cellVoltagesArray.map(x => Math.pow(x - cellVoltagesArray.sum / cellVoltagesArray.length, 2)).sum / cellVoltagesArray.length)).length > 0) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 3)
          } else if (minCellVoltage <= 2200) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 2)
          } else if (minCellVoltage <= 2400) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 1)
          }
        }
      }
    }
  }

  /**
   * 动态压差
   *
   * @param json
   */
  def isDynamicDifferential(json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val current: Integer = json.getInteger("current")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值

        if (maxCellVoltage >= 3150 && maxCellVoltage <= 3500 && minCellVoltage >= 1500) {
          if ((maxCellVoltage - minCellVoltage) >= (300 + math.abs(3500 - maxCellVoltage) * 0.5) && math.abs(current) <= 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
            return
          } else if ((maxCellVoltage - minCellVoltage) >= ((math.abs(current / 1000) * 2) + 200 + math.abs(3500 - maxCellVoltage) * 0.5) && math.abs(current) > 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
            return
          }

          if ((maxCellVoltage - minCellVoltage) >= (250 + math.abs(3500 - maxCellVoltage) * 0.4) && math.abs(current) <= 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
            return
          } else if ((maxCellVoltage - minCellVoltage) >= ((math.abs(current / 1000) * 2) + 150 + math.abs(3500 - maxCellVoltage) * 0.4) && math.abs(current) > 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
            return
          }

          if ((maxCellVoltage - minCellVoltage) >= (200 + math.abs(3500 - maxCellVoltage) * 0.3) && math.abs(current) <= 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
            return
          } else if ((maxCellVoltage - minCellVoltage) >= ((math.abs(current / 1000) * 2) + 100 + math.abs(3500 - maxCellVoltage) * 0.3) && math.abs(current) > 50000) {
            json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
            return
          }
        }
      }
    }
  }

  /**
   * 静态压差
   *
   * @param old_json
   * @param json
   */
  def isStaticDifferential(old_json: JSONObject, json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")

    val old_soc: Int = old_json.getIntValue("soc")
    val cur_soc: Int = json.getIntValue("soc")
    val current: Int = json.getIntValue("current")
    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")

    //过滤采集类异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值
        val cellcz = maxCellVoltage - minCellVoltage

        if ((timeStamp - old_timeStamp) >= 3600 &&
          minCellVoltage >= 500 && math.abs(current) <= 3000 && math.abs(cur_soc - old_soc) <= 1) {
          json.put("Standingcondition", JSON.toJSON(cellVoltagesArray))
        }
        if (old_json.containsKey("Standingcondition") && minCellVoltage > 500 && math.abs(current) <= 2000 && math.abs(cur_soc - old_soc) <= 1) {
          val old_cellVoltagesArray: Array[Int] = stringToIntArray(old_json.getString("Standingcondition"))
          if (old_cellVoltagesArray != null && old_cellVoltagesArray.length != 0) {
            val Avg = cellVoltagesArray.sum.toDouble / cellVoltagesArray.length
            val old_Avg = old_cellVoltagesArray.sum.toDouble / old_cellVoltagesArray.length
            if (math.abs(Avg - old_Avg) <= 4) {
              json.put("Standingcondition", JSON.toJSON(cellVoltagesArray))
            }
          }
        }
        if (json.containsKey("Standingcondition")) {
          //三级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.7 + 46) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if (cellcz >= (math.abs(3315 - maxCellVoltage) * 0.7 + 49.5) && maxCellVoltage > 3280 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3280 - maxCellVoltage), 4) / 12 + 74) && maxCellVoltage > 3200 && maxCellVoltage <= 3280 && (3280 - maxCellVoltage) >= 12)
            || (cellcz >= (math.abs(3280 - maxCellVoltage) + 74) && maxCellVoltage > 3200 && maxCellVoltage <= 3280 && (3280 - maxCellVoltage) < 12)
            || (minCellVoltage < 2000 && maxCellVoltage > 3200 && maxCellVoltage <= 3280)) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          }
          //二级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.6 + 42) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if (cellcz >= (math.abs(3310 - maxCellVoltage) * 0.5 + 43) && maxCellVoltage > 3270 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3270 - maxCellVoltage), 4) / 20 + 63) && maxCellVoltage > 3200 && maxCellVoltage <= 3270 && (3270 - maxCellVoltage) >= 20)
            || (cellcz >= (math.abs(3270 - maxCellVoltage) + 63) && maxCellVoltage > 3200 && maxCellVoltage <= 3270 && (3270 - maxCellVoltage) < 20)
            || (minCellVoltage < 2200 && maxCellVoltage > 3200 && maxCellVoltage <= 3270)) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          }
          //一级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.5 + 38) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          } else if (cellcz >= (math.abs(3300 - maxCellVoltage) * 0.25 + 38) && maxCellVoltage > 3240 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3240 - maxCellVoltage), 4) / 16 + 53) && maxCellVoltage > 3195 && maxCellVoltage <= 3240 && (3240 - maxCellVoltage) >= 16)
            || (cellcz >= (math.abs(3240 - maxCellVoltage) + 53) && maxCellVoltage > 3195 && maxCellVoltage <= 3240 && (3240 - maxCellVoltage) < 16)) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          }
        }
      }
    }
  }

  /**
   * 总压过压
   *
   * @param json
   */
  def isDeviceTypeOverVoltage(json: JSONObject) {

    var totalVoltage: Integer = json.getInteger("totalVoltage")
    val cellCount: Integer = json.getInteger("cellCount")
    //过滤单体异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) { //都为异常值不做此故障判定
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length //排除异常值之后求平均值
        if (totalVoltage != null && cellCount != null) {
          totalVoltage = avgCellVoltages * cellCount //用正常值的平均值补给异常值算总压
          if ((cellVoltagesArray.min > 3600 && (cellVoltagesArray.max - cellVoltagesArray.min < (150 + (cellVoltagesArray.min - 3600) * 0.5))) || avgCellVoltages > 3750)
            return
          if (totalVoltage >= (3680 * cellCount)) {
            json.put(AlarmEnum.deviceTypeOverVoltage.toString, 3)
          } else if (totalVoltage >= (3650 * cellCount)) {
            json.put(AlarmEnum.deviceTypeOverVoltage.toString, 2)
          } else if (totalVoltage >= (3630 * cellCount)) {
            json.put(AlarmEnum.deviceTypeOverVoltage.toString, 1)
          }
        }
      }
    }
  }

  /**
   * 总压欠压
   *
   * @param json
   */
  def isDeviceTypeUnderVoltage(json: JSONObject) {

    var totalVoltage: Integer = json.getInteger("totalVoltage")
    val cellCount: Integer = json.getInteger("cellCount")
    var minTemperature: Integer = json.getInteger("minTemperature")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    //过滤
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) { //都为异常值不做此故障判定
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length //排除异常值之后求平均值

        if (totalVoltage != null && cellCount != null) {
          totalVoltage = avgCellVoltages * cellCount //用正常值的平均值补给异常值算总压
          if (totalVoltage <= (2400 * cellCount) && minTemperature > 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 3)
          } else if (totalVoltage <= (2200 * cellCount) && minTemperature <= 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 3)
          } else if (totalVoltage <= (2300 * cellCount) && minTemperature <= 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 2)
          } else if (totalVoltage <= (2500 * cellCount) && minTemperature > 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 2)
          } else if (totalVoltage <= (2400 * cellCount) && minTemperature <= 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 1)
          } else if (totalVoltage <= (2600 * cellCount) && minTemperature > 0) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 1)
          }
        }
      }
    }
  }

  /**
   * soc虚高
   *
   * @param old_json
   * @param json
   */
  def isSocVirtualHigh(old_json: JSONObject, json: JSONObject, socDataRC: TreeMap[Int, ArrayBuffer[(Int, Float)]]) {

    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")
    val soc: Integer = json.getInteger("soc")
    val old_soc: Integer = old_json.getInteger("soc")
    var batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    var batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")
    val current: Integer = json.getInteger("current") //电流
    var minTemperature: Integer = json.getInteger("minTemperature")
    //过滤单体异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤温度异常值的影响
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    val delta_time: Long = timeStamp - old_timeStamp
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      if (cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) { //都为异常值不做此故障判定
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        batteryMaxVoltage = cellVoltagesArray.max //再取最大值
        batteryMinVoltage = cellVoltagesArray.min //再取最小值

        if (delta_time >= 3600
          && batteryMinVoltage > 500 && math.abs(current) <= 3000 && math.abs(soc - old_soc) < 1) {
          json.put("isSocVirtualHigh", JSON.toJSON(cellVoltagesArray))
        }

        if (old_json.containsKey("isSocVirtualHigh")
          && batteryMinVoltage > 500 && math.abs(current) <= 2000 && math.abs(soc - old_soc) < 1) {
          val old_cellVoltagesArray: Array[Int] = stringToIntArray(old_json.getString("isSocVirtualHigh"))
          if (old_cellVoltagesArray != null && old_cellVoltagesArray.length != 0) {
            val Avg = cellVoltagesArray.sum.toDouble / cellVoltagesArray.length
            val old_Avg = old_cellVoltagesArray.sum.toDouble / old_cellVoltagesArray.length
            if (math.abs(Avg - old_Avg) <= 3) {
              json.put("isSocVirtualHigh", JSON.toJSON(cellVoltagesArray))
            }
          }
        }
        if (json.containsKey("isSocVirtualHigh")) {
          if (minTemperature >= 0 && minTemperature <= 55) {
            val socMax = calcSoc(socDataRC, batteryMaxVoltage, minTemperature)
            if (socMax > 0 && socMax <= 25) {
              val socMin = calcSoc(socDataRC, batteryMinVoltage, minTemperature)
              var socReal: Int = 0
              if (100 - socMax + socMin != 0)
                socReal = 100 * socMin / (100 - socMax + socMin)
              if (old_json.containsKey("socHighAlarmCount")) {
                if (soc - socReal >= 50) {
                  json.put(AlarmEnum.socHigh.toString, 2)
                } else if (soc - socReal >= 35) {
                  json.put(AlarmEnum.socHigh.toString, 1)
                }
              } else {
                if (soc - socReal >= 50) {
                  json.put("socHighAlarmCount", 2)
                } else if (soc - socReal >= 35) {
                  json.put("socHighAlarmCount", 1)
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * soc过低
   *
   * @param json
   */
  def isSocLow(json: JSONObject) {

    val soc: Integer = json.getInteger("soc")
    if (soc != null && soc >= 0 && soc < 3) {
      json.put(AlarmEnum.socLow.toString, 1)
    }
  }

  /**
   * soc跳变
   *
   * @param old_json
   * @param json
   */
  def isSocJump(old_json: JSONObject, json: JSONObject): Unit = {

    val old_soc: Integer = old_json.getInteger("soc")
    val cur_soc: Integer = json.getInteger("soc")
    val old_time: Long = old_json.getLong("timeStamp")
    val cur_time: Long = json.getLong("timeStamp")
    val dif_time: Long = cur_time - old_time

    //SOC陡降
    if (dif_time > 0 && dif_time <= 15 && old_soc != 0 && cur_soc != 100) {
      if (old_json.containsKey("soc_jump_plunge") && old_json.containsKey("soc_jump_plungeCount")) {
        if (cur_soc - old_soc <= 1) { //考虑恰好减速带来的电机反转能量回收情况
          var soc_jump_plungeCount: Integer = old_json.getInteger("soc_jump_plungeCount")
          soc_jump_plungeCount = soc_jump_plungeCount + 1
          if (soc_jump_plungeCount >= 5) {
            json.put(AlarmEnum.socJump.toString, old_json.getInteger("soc_jump_plunge"))
            json.remove("soc_jump_plungeCount")
            json.remove("soc_jump_plunge")
          } else {
            json.put("soc_jump_plungeCount", soc_jump_plungeCount)
            json.put("soc_jump_plunge", old_json.getInteger("soc_jump_plunge"))
          }
        }
      }
      if (old_soc - cur_soc >= 30 && !old_json.containsKey("soc_jump_zoom")) {
        json.put("soc_jump_plunge", 2)
        json.put("soc_jump_plungeCount", 0)
      } else if (old_soc - cur_soc >= 20 && !old_json.containsKey("soc_jump_zoom")) {
        json.put("soc_jump_plunge", 1)
        json.put("soc_jump_plungeCount", 0)
      }
    }

    //SOC陡升
    if (dif_time > 0 && dif_time <= 15 && old_soc != 0 && cur_soc != 100) { //排除平台数据延迟与补发的影响
      if (old_json.containsKey("soc_jump_zoom") && old_json.containsKey("soc_jump_zoomCount")) {
        if (cur_soc - old_soc >= -1) {
          var soc_jump_zoomCount: Integer = old_json.getInteger("soc_jump_zoomCount")
          soc_jump_zoomCount = soc_jump_zoomCount + 1
          if (soc_jump_zoomCount >= 5) {
            json.put(AlarmEnum.socJump.toString, old_json.getInteger("soc_jump_zoom"))
            json.remove("soc_jump_zoomCount")
            json.remove("soc_jump_zoom")
          } else {
            json.put("soc_jump_zoomCount", soc_jump_zoomCount)
            json.put("soc_jump_zoom", old_json.getInteger("soc_jump_zoom"))
          }
        }
      }
      if (cur_soc - old_soc >= 30 && !old_json.containsKey("soc_jump_plunge")) {
        json.put("soc_jump_zoom", 2)
        json.put("soc_jump_zoomCount", 0)
      } else if (cur_soc - old_soc >= 20 && !old_json.containsKey("soc_jump_plunge")) {
        json.put("soc_jump_zoom", 1)
        json.put("soc_jump_zoomCount", 0)
      }
    }
  }


  /**
   * 绝缘报警，totalVoltage电压量纲为0.1v  insulationResistance量纲为千欧
   *
   * @param old_json
   * @param json
   */
  def isInsulationAlarm(old_json: JSONObject, json: JSONObject): Unit = {

    val insulationResistance: Integer = json.getInteger("insulationResistance")
    val oldinsulationResistance: Integer = old_json.getInteger("insulationResistance")
    val chargeStatus: Integer = json.getInteger("chargeStatus")
    val oldchargeStatus: Integer = old_json.getInteger("chargeStatus")
    var totalVoltage: Integer = json.getInteger("totalVoltage")
    val cellCount: Integer = json.getInteger("cellCount")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤数据延迟和补发带来的时间问题
    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")
    val current: Integer = json.getInteger("current") //电流
    val oldcurrent: Integer = old_json.getInteger("current")
    val delta_time: Long = timeStamp - old_timeStamp

    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) { //都为异常值不做此故障判定
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length
        if (totalVoltage != null && cellCount != null) {
          totalVoltage = avgCellVoltages * cellCount //用正常值的平均值补给异常值算总压

          //定义三个计数器，分别用于判断绝缘报警的一二三级
          var insulationAlarmCount1 = 0
          var insulationAlarmCount2 = 0
          var insulationAlarmCount3 = 0
          if (delta_time >= 0 && delta_time <= 60) {
            insulationAlarmCount1 = old_json.getIntValue("insulationAlarmCount1")
            insulationAlarmCount2 = old_json.getIntValue("insulationAlarmCount2")
            insulationAlarmCount3 = old_json.getIntValue("insulationAlarmCount3")
          }
          if (chargeStatus == 3 && insulationResistance > 0 && insulationResistance < 0.5 * totalVoltage * 0.001) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            } else {
              insulationAlarmCount3 += 1
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            }
          } else if (chargeStatus == 1 && delta_time > 0
            && old_json.getIntValue("insulationAlarmCount3") > 1 && insulationResistance > 0 &&
            insulationResistance <= 0.5 * totalVoltage * 0.001) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            } else {
              insulationAlarmCount3 += 1
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            }
          } else if (chargeStatus == 3 && insulationResistance < 0.6 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            } else {
              insulationAlarmCount2 += 1
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            }
          } else if (chargeStatus == 1 && oldchargeStatus == 1 && oldcurrent < -5000 && current < -5000 && oldinsulationResistance > 500
            && insulationResistance <= 0.2 * totalVoltage * 0.001 && insulationResistance > 0) {
            insulationAlarmCount2 += 2
            json.put("insulationAlarmCount2", insulationAlarmCount2)
          } else if (delta_time > 0 && old_json.getIntValue("insulationAlarmCount2") > 1
            && insulationResistance <= 0.2 * totalVoltage * 0.001 && insulationResistance > 0 && chargeStatus == 1
            && oldcurrent < -5000 && current < -5000) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            } else {
              insulationAlarmCount2 += 1
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            }
          } else if (chargeStatus == 3 && insulationResistance < 0.8 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            } else {
              insulationAlarmCount1 += 1
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            }
          } else if (chargeStatus == 1 && insulationResistance <= 0.2 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            } else {
              insulationAlarmCount1 += 1
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            }
          } else {
            return
          }
          if (insulationAlarmCount3 >= 3) {
            json.put(AlarmEnum.insulation.toString, 3)
            if (insulationAlarmCount3 > 5) {
              json.put("insulationAlarmCount3", 2)
            }
          } else if (insulationAlarmCount2 >= 3) {
            json.put(AlarmEnum.insulation.toString, 2)
            if (insulationAlarmCount2 > 5) {
              json.put("insulationAlarmCount2", 2)
            }
          } else if (insulationAlarmCount1 >= 3) {
            json.put(AlarmEnum.insulation.toString, 1)
            json.put("insulationAlarmCount1", 0)
          }
        }
      }
    }
  }

  /**
   * 温差报警
   *
   * @param json
   * @return
   */
  def isBatteryDiffTemperature(json: JSONObject) = {
    var maxTemperature: Integer = json.getInteger("maxTemperature")
    var minTemperature: Integer = json.getInteger("minTemperature")
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    var temperatureDiff: Integer = 0
    //过滤
    if (probeTemperaturesArray != null) {
      //取值判定
      if (probeTemperaturesArray.nonEmpty) {
        maxTemperature = probeTemperaturesArray.max - 40
        minTemperature = probeTemperaturesArray.min - 40
        temperatureDiff = maxTemperature - minTemperature
        if (temperatureDiff >= 25) {
          json.put(AlarmEnum.temperatureDifferential.toString, 3)
        } else if (temperatureDiff >= 20) {
          json.put(AlarmEnum.temperatureDifferential.toString, 2)
        } else if (temperatureDiff >= 18) {
          json.put(AlarmEnum.temperatureDifferential.toString, 1)
        }
      }
    }
  }

  /**
   * 解析json数组
   *
   * @param str
   * @return
   */
  def stringToIntArray(str: String): Array[Int] = {
    // println(str)
    if (str != null && str.length > 2) {
      val strArr: Array[String] = str.substring(1, str.length - 1).split(",")
      val intArr = new Array[Int](strArr.length);
      for (i <- 0 until intArr.length) {
        intArr(i) = strArr(i).toInt
      }
      intArr
    } else {
      null
    }
  }

  /**
   * 电压采集(电压采集线脱落）
   *
   * @param json
   */
  def isVoltagelinefFall(json: JSONObject): Unit = {
    var cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val insulationResistance: Integer = json.getInteger("insulationResistance")
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.filter(_ == 0).length > 0) {
        if (cellVoltagesArray.filter(_ != 0).length > 0) {
          val avgCellVoltage = cellVoltagesArray.filter(_ != 0).sum.toDouble / cellVoltagesArray.filter(_ != 0).length
          if (avgCellVoltage >= 2000) {
            if (!json.containsKey("WakeupfluctuationsVoltage") && insulationResistance > 200) {
              if (json.containsKey("getwelltime")) {
                val getwelltime = json.getLong("getwelltime")
                val timeStamp: Long = json.getLong("timeStamp")
                if (timeStamp > getwelltime)
                  json.put(AlarmEnum.voltageLineFall.toString, 2)
              } else
                json.put(AlarmEnum.voltageLineFall.toString, 2)
            }
            cellVoltagesArray = cellVoltagesArray.filter(_ != 0)
            json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
          }
        } else { //都为异常值的情况
          if (!json.containsKey("WakeupfluctuationsVoltage") && insulationResistance > 200) {
            if (json.containsKey("getwelltime")) {
              val getwelltime = json.getLong("getwelltime")
              val timeStamp: Long = json.getLong("timeStamp")
              if (timeStamp > getwelltime)
                json.put(AlarmEnum.voltageLineFall.toString, 1)
            } else
              json.put(AlarmEnum.voltageLineFall.toString, 1)
          }
          json.put("cellVoltages", null)
        }
      }
    }
  }

  /**
   * 温度采集(温度采集线脱落)
   *
   * @param json
   */
  def isTempLineFall(json: JSONObject): Unit = {
    var probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    val insulationResistance: Integer = json.getInteger("insulationResistance")

    if (probeTemperaturesArray != null) {
      if (probeTemperaturesArray.filter(_ == 0).length > 0) {
        if (probeTemperaturesArray.filter(_ != 0).length > 0) {
          val avgTemperatures = probeTemperaturesArray.filter(_ != 0).sum.toDouble / probeTemperaturesArray.filter(_ != 0).length
          if (avgTemperatures >= 15) {
            if (!json.containsKey("WakeupfluctuationsTemp") && insulationResistance > 200 && !json.containsKey("FlagSix0")) {
              if (json.containsKey("getwelltimeT")) {
                val getwelltimeT = json.getLong("getwelltimeT")
                val timeStamp: Long = json.getLong("timeStamp")
                if (timeStamp > getwelltimeT)
                  json.put(AlarmEnum.tempLineFall.toString, 2)
              } else
                json.put(AlarmEnum.tempLineFall.toString, 2)
            }
            probeTemperaturesArray = probeTemperaturesArray.filter(_ != 0)
            json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
          }
        } else {
          if (!json.containsKey("WakeupfluctuationsTemp") && insulationResistance > 200 && !json.containsKey("FlagSix0")) {
            if (json.containsKey("getwelltimeT")) {
              val getwelltimeT = json.getLong("getwelltimeT")
              val timeStamp: Long = json.getLong("timeStamp")
              if (timeStamp > getwelltimeT)
                json.put(AlarmEnum.tempLineFall.toString, 1)
            } else
              json.put(AlarmEnum.tempLineFall.toString, 1)
          }
          json.put("probeTemperatures", null)
        }
      }
    }
  }

  /**
   * 电压数据异常
   *
   * @param json
   */
  def isVoltageAbnormal(json: JSONObject): Unit = {
    var cellVoltagesArray = stringToIntArray(json.getString("cellVoltages"))
    //    val current: Integer = json.getInteger("current")
    //    val index_arr = ArrayBuffer[Int]()

    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.filter(_ >= 4800).length > 0) {
        if (cellVoltagesArray.filter(_ < 4800).filter(_ != 0).length > 0) {
          val avgCellVoltages = cellVoltagesArray.filter(_ < 4800).filter(_ != 0).sum.toDouble / cellVoltagesArray.filter(_ < 4800).filter(_ != 0).length
          if (avgCellVoltages <= 3600) {
            if (json.containsKey("getwelltime")) {
              val getwelltime = json.getLong("getwelltime")
              val timeStamp: Long = json.getLong("timeStamp")
              if (timeStamp > getwelltime)
                json.put(AlarmEnum.abnormalVoltage.toString, 2)
            } else
              json.put(AlarmEnum.abnormalVoltage.toString, 2)
            cellVoltagesArray = cellVoltagesArray.filter(_ < 4800)
            json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
          }
        } else {
          if (json.containsKey("getwelltime")) {
            val getwelltime = json.getLong("getwelltime")
            val timeStamp: Long = json.getLong("timeStamp")
            if (timeStamp > getwelltime)
              json.put(AlarmEnum.abnormalVoltage.toString, 1)
          } else
            json.put(AlarmEnum.abnormalVoltage.toString, 1)
          json.put("cellVoltages", null)
        }
      }

      //      if (cellVoltagesArray.filter(_ != 0).length > 0 && math.abs(current) < 5000
      //        && (cellVoltagesArray.filter(_ != 0).max - cellVoltagesArray.filter(_ != 0).min) > 500
      //        && (cellVoltagesArray.filter(_ != 0).filter(_ < 2500).length >= 2)) {
      //
      //        //取出异常数据及索引
      //        val cell_index = cellVoltagesArray.zipWithIndex.filter(_._1 != 0).filter(_._1 < 2500)
      //
      //        //判断是否连续
      //        for (i <- 0 until cell_index.length) {
      //          if (((i != (cell_index.length - 1)) && (cell_index(i + 1)._2 - cell_index(i)._2 == 1) && (math.abs(cell_index(i + 1)._1 - cell_index(i)._1) <= 100))
      //            || (i != 0 && ((cell_index(i)._2 - cell_index(i - 1)._2) == 1) && (math.abs(cell_index(i)._1 - cell_index(i - 1)._1) <= 100))) {
      //            index_arr.append(cell_index(i)._2)
      //          }
      //        }
      //
      //        if (index_arr.nonEmpty) {
      //          json.put(AlarmEnum.abnormalVoltage.toString, 1)
      //          //删除连续异常数据
      //          cellVoltagesArray = cellVoltagesArray.zipWithIndex.filter(x => !index_arr.contains(x._2)).map(_._1)
      //          json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
      //        }
      //      }
    } else if (!json.containsKey("Wakeupfluctuations")) {
      if (json.containsKey("getwelltime")) {
        val getwelltime = json.getLong("getwelltime")
        val timeStamp: Long = json.getLong("timeStamp")
        if (timeStamp > getwelltime)
          json.put(AlarmEnum.abnormalVoltage.toString, 1)
      } else
        json.put(AlarmEnum.abnormalVoltage.toString, 1)
    }
  }

  /**
   * 温度数据异常
   *
   * @param json
   */
  def isTempAbnormal(json: JSONObject): Unit = {
    var probeTemperaturesArray = stringToIntArray(json.getString("probeTemperatures"))
    if (probeTemperaturesArray != null) {
      if (probeTemperaturesArray.filter(_ >= 165).length > 0) {
        if (probeTemperaturesArray.filter(_ != 0).filter(_ < 165).length > 0) {
          val avgprobeTemperatures = probeTemperaturesArray.filter(_ != 0).filter(_ < 165).sum.toDouble / probeTemperaturesArray.filter(_ != 0).filter(_ < 165).length
          if (avgprobeTemperatures < 100) {
            if (json.containsKey("getwelltimeT")) {
              val getwelltimeT = json.getLong("getwelltimeT")
              val timeStamp: Long = json.getLong("timeStamp")
              if (timeStamp > getwelltimeT)
                json.put(AlarmEnum.abnormalTemperature.toString, 2)
            } else
              json.put(AlarmEnum.abnormalTemperature.toString, 2)
            probeTemperaturesArray = probeTemperaturesArray.filter(_ < 165)
            json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
          }
        } else {
          if (json.containsKey("getwelltimeT")) {
            val getwelltimeT = json.getLong("getwelltimeT")
            val timeStamp: Long = json.getLong("timeStamp")
            if (timeStamp > getwelltimeT)
              json.put(AlarmEnum.abnormalTemperature.toString, 1)
          } else
            json.put(AlarmEnum.abnormalTemperature.toString, 1)
          json.put("probeTemperatures", null)
        }
      }

      //      if (probeTemperaturesArray != null && probeTemperaturesArray.filter(_ != 0).length >= 10 && probeTemperaturesArray.filter(_ != 0).max < 95
      //        && probeTemperaturesArray.filter(_ != 0).max - probeTemperaturesArray.filter(_ != 0).min > 10) {
      //        val AvgTemperature = probeTemperaturesArray.filter(_ != 0).sum.toDouble / probeTemperaturesArray.filter(_ != 0).length
      //        val DetalAvg = AvgTemperature - probeTemperaturesArray.filter(_ != 0).min
      //        if (DetalAvg > 9 && probeTemperaturesArray.filter(_ != 0).filter(_ < (AvgTemperature - (DetalAvg / 2))).length <
      //          probeTemperaturesArray.filter(_ != 0).filter(_ < (AvgTemperature - 9)).length * 2
      //          && probeTemperaturesArray.filter(_ != 0).filter(_ > (AvgTemperature + (DetalAvg / 2))).length <
      //          probeTemperaturesArray.filter(_ != 0).filter(_ < (AvgTemperature - (DetalAvg / 2))).length
      //          && (probeTemperaturesArray.filter(_ != 0).max - AvgTemperature) * 2 < DetalAvg) {
      //          json.put(AlarmEnum.abnormalTemperature.toString, 1)
      //          for (i <- 0 until probeTemperaturesArray.length) {
      //            if (probeTemperaturesArray(i) != 0 && probeTemperaturesArray(i) < AvgTemperature - 9)
      //              probeTemperaturesArray(i) = AvgTemperature.toInt
      //          }
      //          json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
      //        }
      //      }
    } else if (!json.containsKey("Wakeupfluctuations")) {
      if (json.containsKey("getwelltimeT")) {
        val getwelltimeT = json.getLong("getwelltimeT")
        val timeStamp: Long = json.getLong("timeStamp")
        if (timeStamp > getwelltimeT)
          json.put(AlarmEnum.abnormalTemperature.toString, 1)
      } else
        json.put(AlarmEnum.abnormalTemperature.toString, 1)
    }
  }

  /**
   * 相邻单体采集异常
   *
   * @param json
   */
  def isCellVoltageNeighborFault(json: JSONObject): Unit = {
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null) {
      RecursiveCellVoltageNeighbor(json, cellVoltagesArray)
    }
  }

  /**
   * 相邻单体递归函数
   *
   * @param json
   * @param cellVoltagesArray
   * @return
   */
  private def RecursiveCellVoltageNeighbor(json: JSONObject, cellVoltagesArray: Array[Int]): Unit = {
    if (cellVoltagesArray.nonEmpty) {
      val max = cellVoltagesArray.max
      val min = cellVoltagesArray.min
      val maxIndex = cellVoltagesArray.zipWithIndex.max._2 //最大电压所在序号（3.3，1）（3.2，2）前为电压值，后为所在序号
      val delta = math.abs(max - min)
      if (max >= 3000 && min <= 3400 && delta > 200) { //差值过滤，排除大电流带来的影响
        //除去最后一个单体为最高电压的情况，右侧判断
        if (maxIndex != (cellVoltagesArray.length - 1)) {
          if (math.abs(max - cellVoltagesArray(maxIndex + 1)) >= 0.95 * delta) { //0.95是兼容多组故障的影响
            val avg = Math.abs(max + cellVoltagesArray(maxIndex + 1)) / 2
            if (cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).nonEmpty) { //去除最高电压和最后一个单体为空的情况
              val avgCell = cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).sum / cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).length //排除多组故障且最高电压相等的情况干扰
              if (math.abs(avg - avgCell) <= 50 && avg <= 3400 && avgCell <= 3400) { //排除动态干扰
                json.put(AlarmEnum.isAdjacentMonomerAbnormal.toString, 2)
                val isCellVoltageNeighborArray: Array[Int] = Array(avgCell, avgCell) //单体平均电压作为数组写入两次
                val tempCellVoltagesArray = cellVoltagesArray.patch(maxIndex, isCellVoltageNeighborArray, 2) //最大值的单体序号，和右测的相邻序号以平均值替换，连续替换两个
                json.put("cellVoltages", JSON.toJSON(tempCellVoltagesArray))
                RecursiveCellVoltageNeighbor(json, tempCellVoltagesArray)
              }
            }
          }
        }

        //除去第一个单体为最高电压的情况，左侧判断
        if (maxIndex != 0) {
          if (math.abs(max - cellVoltagesArray(maxIndex - 1)) >= 0.95 * delta) {
            val avg = Math.abs(max + cellVoltagesArray(maxIndex - 1)) / 2
            if (cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).nonEmpty) {
              val avgCell = cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).sum / cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).length
              if (math.abs(avg - avgCell) <= 50 && avg <= 3400 && avgCell <= 3400) {
                json.put(AlarmEnum.isAdjacentMonomerAbnormal.toString, 2)
                val isCellVoltageNeighborArray: Array[Int] = Array(avgCell, avgCell)
                val tempCellVoltagesArray = cellVoltagesArray.patch(maxIndex - 1, isCellVoltageNeighborArray, 2)
                json.put("cellVoltages", JSON.toJSON(tempCellVoltagesArray))
                RecursiveCellVoltageNeighbor(json, tempCellVoltagesArray)
              }
            }
          }
        }
      }
    }
  }


  /**
   * 从机掉线
   *
   * @param json
   * @return
   */
  def isSlavedisconnect(json: JSONObject): Unit = {
    var cellVoltagesArray = stringToIntArray(json.getString("cellVoltages"))
    var probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    val insulationResistance: Integer = json.getInteger("insulationResistance")
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      if (cellVoltagesArray.length == 96 && probeTemperaturesArray.length == 12
        && ((cellVoltagesArray.slice(0, 48).filter(_ != 5000).filter(_ != 0).length == 0 && probeTemperaturesArray.slice(0, 6).filter(_ != 0).length == 0)
        || (cellVoltagesArray.slice(48, 96).filter(_ != 5000).filter(_ != 0).length == 0 && probeTemperaturesArray.slice(6, 12).filter(_ != 0).length == 0))) {
        if (!json.containsKey("WakeupfluctuationsTemp") && !json.containsKey("WakeupfluctuationsVoltage") && insulationResistance > 200) {
          if (json.containsKey("getwelltime") || json.containsKey("getwelltimeT")) {
            val getwelltime = json.getLongValue("getwelltime")
            val getwelltimeT = json.getLongValue("getwelltimeT")
            val timeStamp: Long = json.getLong("timeStamp")
            if (timeStamp > getwelltime && timeStamp > getwelltimeT)
              json.put(AlarmEnum.slaveDisconnect.toString, 2)
          } else
            json.put(AlarmEnum.slaveDisconnect.toString, 2)
        }
        if (cellVoltagesArray.slice(0, 48).filter(_ != 5000).filter(_ != 0).length == 0) {
          cellVoltagesArray = cellVoltagesArray.slice(48, 96)
          probeTemperaturesArray = probeTemperaturesArray.slice(6, 12)
        } else {
          cellVoltagesArray = cellVoltagesArray.slice(0, 48)
          probeTemperaturesArray = probeTemperaturesArray.slice(0, 6)
        }
        json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
        json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
      }
      if (cellVoltagesArray.length == 104 && probeTemperaturesArray.length == 13
        && ((cellVoltagesArray.slice(0, 48).filter(_ != 5000).filter(_ != 0).length == 0 && probeTemperaturesArray.slice(0, 6).filter(_ != 0).length == 0)
        || (cellVoltagesArray.slice(48, 104).filter(_ != 5000).filter(_ != 0).length == 0 && probeTemperaturesArray.slice(6, 13).filter(_ != 0).length == 0))) {
        if (!json.containsKey("WakeupfluctuationsTemp") && !json.containsKey("WakeupfluctuationsVoltage")) {
          if (json.containsKey("getwelltime") || json.containsKey("getwelltimeT")) {
            val getwelltime = json.getLongValue("getwelltime")
            val getwelltimeT = json.getLongValue("getwelltimeT")
            val timeStamp: Long = json.getLong("timeStamp")
            if (timeStamp > getwelltime && timeStamp > getwelltimeT)
              json.put(AlarmEnum.slaveDisconnect.toString, 2)
          } else
            json.put(AlarmEnum.slaveDisconnect.toString, 2)
        }
        if (cellVoltagesArray.slice(0, 48).filter(_ != 5000).filter(_ != 0).length == 0) {
          cellVoltagesArray = cellVoltagesArray.slice(48, 104)
          probeTemperaturesArray = probeTemperaturesArray.slice(6, 13)
        } else {
          cellVoltagesArray = cellVoltagesArray.slice(0, 48)
          probeTemperaturesArray = probeTemperaturesArray.slice(0, 6)
        }
        json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
        json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
      }
    }

    if (cellVoltagesArray != null && probeTemperaturesArray != null && cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) {
      if (cellVoltagesArray.length == 46 && probeTemperaturesArray.length == 13 && probeTemperaturesArray(12) == 0) {
        probeTemperaturesArray = probeTemperaturesArray.slice(0, 12)
        json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
      }
      if (cellVoltagesArray.length == 103 && probeTemperaturesArray.length == 13 && probeTemperaturesArray(12) == 0 && cellVoltagesArray.slice(96, 103).filter(_ != 5000).length == 0) {
        probeTemperaturesArray = probeTemperaturesArray.slice(0, 12)
        cellVoltagesArray = cellVoltagesArray.slice(0, 96)
        json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
        json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
      }
      if (cellVoltagesArray.length == 96 && probeTemperaturesArray.length == 12) {
        if ((probeTemperaturesArray(7) == 0 || probeTemperaturesArray(6) == 0) && probeTemperaturesArray.filter(_ == 0).length == 1) {
          probeTemperaturesArray = probeTemperaturesArray.filter(_ != 0)
          json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
        }
        if (cellVoltagesArray(0) == 65376 && cellVoltagesArray(1) == 65535 && cellVoltagesArray(2) == 65535 && probeTemperaturesArray(0) == 12 && probeTemperaturesArray(1) == 255 && probeTemperaturesArray(2) == 255) {
          probeTemperaturesArray = probeTemperaturesArray.slice(3, 12)
          cellVoltagesArray = cellVoltagesArray.slice(3, 96)
          json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
          json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
        }
        if (cellVoltagesArray(cellVoltagesArray.length - 1) == 5000) {
          cellVoltagesArray = cellVoltagesArray.slice(0, cellVoltagesArray.length - 1)
          json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
        }
        if (cellVoltagesArray(47) == 0) {
          cellVoltagesArray = cellVoltagesArray.filter(_ != 0)
          json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
        }
      }
      if (cellVoltagesArray.length == 104 && probeTemperaturesArray.length == 13 && cellVoltagesArray.slice(96, 104).filter(_ != 5000).length == 0 && probeTemperaturesArray(12) == 0) {
        probeTemperaturesArray = probeTemperaturesArray.slice(0, 12)
        cellVoltagesArray = cellVoltagesArray.slice(0, 96)
        json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
        json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
      }
    }
  }
}
