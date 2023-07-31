package geely

import `enum`.AlarmEnum
import com.alibaba.fastjson.{JSON, JSONObject}
import coom.AdvancedFuncs.notSocLayerLFP
import coom.VoltageFuncs.processVoltageLayer
import utils.CommonFuncs.{stringToDoubleArray, stringToIntArray}
import utils.MathFuncs.calcSoc

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

object ModelFunction_LFP extends Serializable {
  /**
   * 高温报警
   *
   * @param json
   */
  def isBatteryHighTemperature(json: JSONObject) {

    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    var maxTemperature: Integer = json.getInteger("maxTemperature")
    //过滤
    if (probeTemperaturesArray != null) {
      //取值判定
      if (probeTemperaturesArray.nonEmpty) {
        maxTemperature = probeTemperaturesArray.max - 40 //再取最大值

        if (maxTemperature >= 60 && maxTemperature < 125) {
          json.put(AlarmEnum.batteryHighTemperature.toString, 3)
        } else if (maxTemperature > 56 && maxTemperature < 60) {
          json.put(AlarmEnum.batteryHighTemperature.toString, 2)
        } else if (maxTemperature > 53 && maxTemperature <= 56 && maxTemperature - (probeTemperaturesArray.min - 40) > 8) {
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

        if (maxCellVoltage > 3800 && maxCellVoltage < 4000) {
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
        if (minTemperature > 20 && minTemperature < 125) { //只有都为0V的时候才能跳过前面的过滤，所以剔除
          if (minCellVoltage <= 2300
            && cellVoltagesArray.filter(x => (cellVoltagesArray.sum / cellVoltagesArray.length - x) > 3 * Math.sqrt(cellVoltagesArray.map(x => Math.pow(x - cellVoltagesArray.sum / cellVoltagesArray.length, 2)).sum / cellVoltagesArray.length)).length > 0) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 3) //monomerBatteryUnderVoltage
          } else if (minCellVoltage <= 2450) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 2) //deviceTypeUnderVoltage单体设备欠压
          } else if (minCellVoltage <= 2600) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 1)
          }
        } else if (minTemperature > 10 && minTemperature <= 20) {
          if (minCellVoltage <= 2100
            && cellVoltagesArray.filter(x => (cellVoltagesArray.sum / cellVoltagesArray.length - x) > 3 * Math.sqrt(cellVoltagesArray.map(x => Math.pow(x - cellVoltagesArray.sum / cellVoltagesArray.length, 2)).sum / cellVoltagesArray.length)).length > 0) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 3) //monomerBatteryUnderVoltage
          } else if (minCellVoltage <= 2250) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 2)
          } else if (minCellVoltage <= 2400) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 1)
          }
        } else if (minTemperature <= 10 && minTemperature > -30) {
          if (minCellVoltage <= 1800
            && cellVoltagesArray.filter(x => (cellVoltagesArray.sum / cellVoltagesArray.length - x) > 3 * Math.sqrt(cellVoltagesArray.map(x => Math.pow(x - cellVoltagesArray.sum / cellVoltagesArray.length, 2)).sum / cellVoltagesArray.length)).length > 0) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 3)
          } else if (minCellVoltage <= 2000) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 2)
          } else if (minCellVoltage <= 2200) {
            json.put(AlarmEnum.monomerBatteryUnderVoltage.toString, 1)
          }
        }
      }
    }
  }

  /**
   * 动态压差DJ2136
   *
   * @param json
   */
  def isDynamicDifferential_DJ2136(json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val current: Integer = json.getInteger("current")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值
        val voltageDiff = maxCellVoltage - minCellVoltage
        if (maxCellVoltage >= 3150 && maxCellVoltage <= 3500 && minCellVoltage >= 500) {

          if (math.abs(current) <= 25000) {
            if (voltageDiff >= (300 + (3500 - maxCellVoltage) * 0.5)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
              return
            } else if (voltageDiff >= (250 + (3500 - maxCellVoltage) * 0.4)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
              return
            } else if (voltageDiff >= (200 + (3500 - maxCellVoltage) * 0.3)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
              return
            }
          } else if (math.abs(current) > 25000) {
            if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (250 + (3500 - maxCellVoltage) * 0.5))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
              return
            } else if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (200 + (3500 - maxCellVoltage) * 0.4))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
              return
            } else if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (150 + (3500 - maxCellVoltage) * 0.3))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
              return
            }
          }
        }
      }
    }
  }

  /**
   * 动态压差DJ2137
   *
   * @param json
   */
  def isDynamicDifferential_DJ2137(json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val current: Integer = json.getInteger("current")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值
        val voltageDiff = maxCellVoltage - minCellVoltage
        if (maxCellVoltage >= 3150 && maxCellVoltage <= 3500 && minCellVoltage >= 500) {

          if (math.abs(current) <= 50000) {
            if (voltageDiff >= (300 + (3500 - maxCellVoltage) * 0.5)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
              return
            } else if (voltageDiff >= (250 + (3500 - maxCellVoltage) * 0.4)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
              return
            } else if (voltageDiff >= (200 + (3500 - maxCellVoltage) * 0.3)) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
              return
            }
          } else if (math.abs(current) > 50000) {
            if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (200 + (3500 - maxCellVoltage) * 0.5))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 3)
              return
            } else if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (150 + (3500 - maxCellVoltage) * 0.4))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 2)
              return
            } else if (voltageDiff >= ((math.abs(current) * 2) / 1000 + (100 + (3500 - maxCellVoltage) * 0.3))) {
              json.put(AlarmEnum.batteryConsistencyPoor.toString, 1)
              return
            }
          }
        }
      }
    }
  }

  /**
   * 静态压差DJ2136
   *
   * @param old_json
   * @param json
   */
  def isStaticDifferential_DJ2136(old_json: JSONObject, json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")

    val old_soc: Int = old_json.getIntValue("soc")
    val cur_soc: Int = json.getIntValue("soc")
    val current: Int = json.getIntValue("current")
    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")

    //过滤采集类异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val old_cellVoltagesArray: Array[Int] = stringToIntArray(old_json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值
        var cellcz = maxCellVoltage - minCellVoltage

        if ((timeStamp - old_timeStamp) >= 3600 &&
          minCellVoltage >= 500 && math.abs(current) <= 3000 && math.abs(cur_soc - old_soc) <= 1) {
          json.put("Standingcondition", JSON.toJSON(cellVoltagesArray))
          // 吉利自唤醒第一帧电压分层优化
          val modifiedArray: Array[Int] = processVoltageLayer(old_cellVoltagesArray, cellVoltagesArray)
          if (modifiedArray != null) {
            json.put("cellVoltages", JSON.toJSON(modifiedArray))
            maxCellVoltage = modifiedArray.max
            minCellVoltage = modifiedArray.min
            cellcz = maxCellVoltage - minCellVoltage
            json.put("Standingcondition", JSON.toJSON(modifiedArray))
          }
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
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.7 + 45) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if (cellcz >= (math.abs(3310 - maxCellVoltage) * 0.3 + 49) && maxCellVoltage > 3265 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3265 - maxCellVoltage), 4) / 14 + 62.5) && maxCellVoltage > 3200 && maxCellVoltage <= 3265 && (3265 - maxCellVoltage) >= 14)
            || (cellcz >= (math.abs(3265 - maxCellVoltage) + 62.5) && maxCellVoltage > 3200 && maxCellVoltage <= 3265 && (3265 - maxCellVoltage) < 14)
            || (minCellVoltage < 1900 && maxCellVoltage > 3200 && maxCellVoltage <= 3265)) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          }
          //二级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.6 + 40) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if (cellcz >= (math.abs(3300 - maxCellVoltage) * 0.3 + 40) && maxCellVoltage > 3250 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3250 - maxCellVoltage), 4) / 18 + 55) && maxCellVoltage > 3200 && maxCellVoltage <= 3250 && (3250 - maxCellVoltage) >= 18)
            || (cellcz >= (math.abs(3250 - maxCellVoltage) + 55) && maxCellVoltage > 3200 && maxCellVoltage <= 3250 && (3250 - maxCellVoltage) < 18)) {
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
          } else if (cellcz >= (math.abs(3290 - maxCellVoltage) * 0.3 + 34) && maxCellVoltage > 3235 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)) {
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3235 - maxCellVoltage), 4) / 13 + 50.5) && maxCellVoltage > 3200 && maxCellVoltage <= 3235 && (3235 - maxCellVoltage) >= 13)
            || (cellcz >= (math.abs(3235 - maxCellVoltage) + 50.5) && maxCellVoltage > 3200 && maxCellVoltage <= 3235 && (3235 - maxCellVoltage) < 13)) {
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
   * 静态压差DJ2137
   *
   * @param old_json
   * @param json
   */
  def isStaticDifferential_DJ2137(old_json: JSONObject, json: JSONObject): Unit = {
    var minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    var maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")

    val old_soc: Int = old_json.getIntValue("soc")
    val cur_soc: Int = json.getIntValue("soc")
    val current: Int = json.getIntValue("current")
    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")

    //过滤采集类异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val old_cellVoltagesArray: Array[Int] = stringToIntArray(old_json.getString("cellVoltages"))
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) {
        maxCellVoltage = cellVoltagesArray.max //再取最大值
        minCellVoltage = cellVoltagesArray.min //再取最小值
        var cellcz = maxCellVoltage - minCellVoltage

        if ((timeStamp - old_timeStamp) >= 3600 &&
          minCellVoltage >= 500 && math.abs(current) <= 3000 && math.abs(cur_soc - old_soc) <= 1) {
          json.put("Standingcondition", JSON.toJSON(cellVoltagesArray))
          // 吉利自唤醒第一帧电压分层优化
          val modifiedArray: Array[Int] = processVoltageLayer(old_cellVoltagesArray, cellVoltagesArray)
          if (modifiedArray != null) {
            json.put("cellVoltages", JSON.toJSON(modifiedArray))
            maxCellVoltage = modifiedArray.max
            minCellVoltage = modifiedArray.min
            cellcz = maxCellVoltage - minCellVoltage
            json.put("Standingcondition", JSON.toJSON(modifiedArray))
          }
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
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.7 + 45) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if (cellcz >= (math.abs(3305 - maxCellVoltage) * 0.6 + 43) && maxCellVoltage > 3255 && maxCellVoltage <= 3320 && notSocLayerLFP(old_json, json)) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3255 - maxCellVoltage), 4) / 17 + 73) && maxCellVoltage > 3200 && maxCellVoltage <= 3255 && (3255 - maxCellVoltage) >= 17)
            || (cellcz >= (math.abs(3255 - maxCellVoltage) + 73) && maxCellVoltage > 3200 && maxCellVoltage <= 3255 && (3255 - maxCellVoltage) < 17)
            || (minCellVoltage < 1900 && maxCellVoltage > 3200 && maxCellVoltage <= 3255)) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 3)
            }
            return
          }
          //二级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.6 + 40) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if (cellcz >= (math.abs(3300 - maxCellVoltage) * 0.3 + 40) && maxCellVoltage > 3240 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3240 - maxCellVoltage), 4) / 30 + 58) && maxCellVoltage > 3170 && maxCellVoltage <= 3240 && (3240 - maxCellVoltage) >= 30)
            || (cellcz >= (math.abs(3240 - maxCellVoltage) + 58) && maxCellVoltage > 3170 && maxCellVoltage <= 3240 && (3240 - maxCellVoltage) < 30)) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 2)
            }
            return
          }
          //一级报警
          if (cellcz >= (math.abs(3330 - maxCellVoltage) * 0.5 + 38) && maxCellVoltage > 3320 && maxCellVoltage <= 3400) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          } else if (cellcz >= (math.abs(3290 - maxCellVoltage) * 0.2 + 37) && maxCellVoltage > 3225 && maxCellVoltage <= 3320) {
            if (notSocLayerLFP(old_json, json)){
              json.put(AlarmEnum.batteryStaticConsistencyPoor.toString, 1)
            }
            return
          } else if ((cellcz >= (math.pow(math.abs(3225 - maxCellVoltage), 4) / 22 + 50) && maxCellVoltage > 3170 && maxCellVoltage <= 3225 && (3225 - maxCellVoltage) >= 22)
            || (cellcz >= (math.abs(3225 - maxCellVoltage) + 50) && maxCellVoltage > 3170 && maxCellVoltage <= 3225 && (3225 - maxCellVoltage) < 22)) {
            if (notSocLayerLFP(old_json, json)){
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
      if (cellVoltagesArray.nonEmpty) {
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length
        if (totalVoltage != null && cellCount != null) {
          totalVoltage = avgCellVoltages * cellCount
          if (totalVoltage >= (3700 * cellCount)) {
            json.put(AlarmEnum.deviceTypeOverVoltage.toString, 3)
          } else if (totalVoltage >= (3670 * cellCount)) {
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
    //过滤单体异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤温度异常值的影响
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    //过滤
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) { //都为异常值不做此故障判定
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length //排除异常值之后求平均值

        if (totalVoltage != null && cellCount != null && minTemperature != null) {
          totalVoltage = avgCellVoltages * cellCount //用正常值的平均值补给异常值算总压
          if (totalVoltage <= (2000 * cellCount) && minTemperature <= 10) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 3)
          } else if (totalVoltage <= (2150 * cellCount) && minTemperature > 10 && minTemperature <= 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 3)
          } else if (totalVoltage <= (2300 * cellCount) && minTemperature > 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 3)
          } else if (totalVoltage <= (2200 * cellCount) && minTemperature <= 10) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 2)
          } else if (totalVoltage <= (2350 * cellCount) && minTemperature > 10 && minTemperature <= 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 2)
          } else if (totalVoltage <= (2500 * cellCount) && minTemperature > 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 2)
          } else if (totalVoltage <= (2300 * cellCount) && minTemperature <= 10) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 1)
          } else if (totalVoltage <= (2500 * cellCount) && minTemperature > 10 && minTemperature <= 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 1)
          } else if (totalVoltage <= (2700 * cellCount) && minTemperature > 20) {
            json.put(AlarmEnum.deviceTypeUnderVoltage.toString, 1)
          }
        }
      }
    }
  }

  /**
   * soc虚高DJ2136
   *
   * @param old_json
   * @param json
   */
  def isSocVirtualHigh_DJ2136(old_json: JSONObject, json: JSONObject,socData:TreeMap[Int, ArrayBuffer[(Int, Float)]]) {

    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")
    val soc: Integer = json.getInteger("soc")
    var batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    var batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")
    val current: Integer = json.getInteger("current") //电流
    var minTemperature: Integer = json.getInteger("minTemperature")
    var maxTemperature: Integer = json.getInteger("maxTemperature")
    val dif_Time: Long = timeStamp - old_timeStamp

    //过滤单体异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤温度异常值的影响
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    //过滤
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) { //都为异常值不做此故障判定
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        maxTemperature = probeTemperaturesArray.max - 40 //再取最大值
        batteryMaxVoltage = cellVoltagesArray.max //再取最大值
        batteryMinVoltage = cellVoltagesArray.min //再取最小值

        if (dif_Time >= 3600 && math.abs(current) <= 3000 && minTemperature >= 0 && minTemperature <= 45) {
          val socMax = calcSoc(socData,batteryMaxVoltage, minTemperature)
          if (socMax > 0 && socMax < 25) {
            val socMin = calcSoc(socData,batteryMinVoltage, minTemperature)
            var socReal: Int = 0
            if (100 - socMax + socMin != 0)
              socReal = 100 * socMin / (100 - socMax + socMin)
            if (soc - socReal >= 45) {
              json.put(AlarmEnum.socHigh.toString, 2)
            } else if (soc - socReal >= 25) { //增大阈值和增加1级故障
              json.put(AlarmEnum.socHigh.toString, 1)
            }
          }

        }
      }
    }
  }

  /**
   * soc虚高DJ2137
   *
   * @param old_json
   * @param json
   */
  def isSocVirtualHigh_DJ2137(old_json: JSONObject, json: JSONObject,socData:TreeMap[Int, ArrayBuffer[(Int, Float)]]) {

    val timeStamp: Long = json.getLong("timeStamp")
    val old_timeStamp: Long = old_json.getLong("timeStamp")
    val soc: Integer = json.getInteger("soc")
    var batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    var batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")
    val current: Integer = json.getInteger("current") //电流
    var minTemperature: Integer = json.getInteger("minTemperature")
    var maxTemperature: Integer = json.getInteger("maxTemperature")
    val dif_Time: Long = timeStamp - old_timeStamp

    //过滤单体异常值的影响
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    //过滤温度异常值的影响
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    //过滤
    if (cellVoltagesArray != null && probeTemperaturesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty && probeTemperaturesArray.nonEmpty) { //都为异常值不做此故障判定
        minTemperature = probeTemperaturesArray.min - 40 //再取最小值
        maxTemperature = probeTemperaturesArray.max - 40 //再取最大值
        batteryMaxVoltage = cellVoltagesArray.max //再取最大值
        batteryMinVoltage = cellVoltagesArray.min //再取最小值

        if (dif_Time >= 3600 && math.abs(current) <= 3000 && minTemperature >= -30 && minTemperature <= 55) {
          val socMax = calcSoc(socData,batteryMaxVoltage, minTemperature)
          if (socMax > 0 && socMax < 25) {
            val socMin = calcSoc(socData,batteryMinVoltage, minTemperature)
            var socReal: Int = 0
            if (100 - socMax + socMin != 0)
              socReal = 100 * socMin / (100 - socMax + socMin)
            if (soc - socReal >= 45) {
              json.put(AlarmEnum.socHigh.toString, 2)
            } else if (soc - socReal >= 25) { //增大阈值和增加1级故障
              json.put(AlarmEnum.socHigh.toString, 1)
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
    val dcstatus: Integer = json.getInteger("dcStatus")
    val old_dcstatus: Integer = old_json.getInteger("dcStatus")
    val dif_time: Long = cur_time - old_time

    //SOC陡降
    if (dif_time > 0 && dif_time <= 35 && old_soc != 0 && cur_soc != 100 && dcstatus == 1 && old_dcstatus == 1) {
      if (old_json.containsKey("soc_jump_plunge") && old_json.containsKey("soc_jump_plungeCount")) {
        if (cur_soc - old_soc <= 1) { //考虑恰好减速带来的电机反转能量回收情况
          var soc_jump_plungeCount: Integer = old_json.getInteger("soc_jump_plungeCount")
          soc_jump_plungeCount = soc_jump_plungeCount + 1
          if (soc_jump_plungeCount >= 4) {
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
    if (dif_time > 0 && dif_time <= 35 && old_soc != 0 && cur_soc != 100 && dcstatus == 1 && old_dcstatus == 1) { //排除平台数据延迟与补发的影响
      if (old_json.containsKey("soc_jump_zoom") && old_json.containsKey("soc_jump_zoomCount")) {
        if (cur_soc - old_soc >= -1) {
          var soc_jump_zoomCount: Integer = old_json.getInteger("soc_jump_zoomCount")
          soc_jump_zoomCount = soc_jump_zoomCount + 1
          if (soc_jump_zoomCount >= 4) {
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
    val oldcurrent: Integer = old_json.getInteger("current")
    val current: Integer = json.getInteger("current")
    var chargeStatus: Integer = json.getInteger("chargeStatus")
    val oldchargeStatus: Integer = old_json.getInteger("chargeStatus")
    var totalVoltage: Integer = json.getInteger("totalVoltage")
    val cellCount: Integer = json.getInteger("cellCount")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val old_timeStamp: Long = old_json.getLong("timeStamp")
    val timeStamp: Long = json.getLong("timeStamp")
    val delta_time: Long = timeStamp - old_timeStamp
    //过滤
    if (cellVoltagesArray != null) {
      //取值判定
      if (cellVoltagesArray.nonEmpty) { //都为异常值不做此故障判定
        val avgCellVoltages = cellVoltagesArray.sum / cellVoltagesArray.length //排除异常值之后求平均值
        if (totalVoltage != null && cellCount != null) {
          totalVoltage = avgCellVoltages * cellCount //用正常值的平均值补给异常值算总压

          //充电转放电延续一帧
          if (oldchargeStatus == 1 && (chargeStatus == 3 || chargeStatus == 4) && delta_time > 0 && delta_time < 60) {
            //            if (delta_time ==1) {
            //              chargeStatus = 1
            //              json.put("chargeStatusCount", 30)
            //            }
            chargeStatus = 1
          }

          //定义三个计数器，分别用于判断绝缘报警的一二三级
          var insulationAlarmCount1 = 0
          var insulationAlarmCount2 = 0
          var insulationAlarmCount3 = 0
          if (delta_time >= 0 && delta_time <= 60) {
            insulationAlarmCount1 = old_json.getIntValue("insulationAlarmCount1")
            insulationAlarmCount2 = old_json.getIntValue("insulationAlarmCount2")
            insulationAlarmCount3 = old_json.getIntValue("insulationAlarmCount3")
          }
          if ((chargeStatus == 3 || chargeStatus == 4) && insulationResistance > 0 && insulationResistance < 0.5 * totalVoltage * 0.001) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            } else {
              insulationAlarmCount3 += 1
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            }
          } else if (chargeStatus == 1 && delta_time > 0
            && old_json.getIntValue("insulationAlarmCount3") > 1 && insulationResistance > 0 &&
            insulationResistance <= 0.2 * totalVoltage * 0.001) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            } else {
              insulationAlarmCount3 += 1
              json.put("insulationAlarmCount3", insulationAlarmCount3)
            }
          } else if ((chargeStatus == 3 || chargeStatus == 4) && insulationResistance < 0.7 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            } else {
              insulationAlarmCount2 += 1
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            }
          } else if (chargeStatus == 1 && oldchargeStatus == 1 && oldcurrent < -5000 && current < -5000 && oldinsulationResistance > 500
            && insulationResistance <= 0.5 * totalVoltage * 0.001 && insulationResistance > 0) {
            insulationAlarmCount2 += 2
            json.put("insulationAlarmCount2", insulationAlarmCount2)
          } else if (delta_time > 0 && (old_json.getIntValue("insulationAlarmCount2") > 1 || old_json.getIntValue("insulationAlarmCount3") > 1)
            && insulationResistance <= 0.5 * totalVoltage * 0.001 && insulationResistance > 0 && chargeStatus == 1
            && oldcurrent < -5000 && current < -5000) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            } else {
              insulationAlarmCount2 += 1
              json.put("insulationAlarmCount2", insulationAlarmCount2)
            }
          } else if ((chargeStatus == 3 || chargeStatus == 4) && insulationResistance < 1.0 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            } else {
              insulationAlarmCount1 += 1
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            }
          } else if (chargeStatus == 1 && insulationResistance <= 0.5 * totalVoltage * 0.001 && insulationResistance > 0) {
            if (insulationResistance == oldinsulationResistance) {
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            } else {
              insulationAlarmCount1 += 1
              json.put("insulationAlarmCount1", insulationAlarmCount1)
            }
          } else {
            return
          }
          if (insulationAlarmCount3 > 2) {
            json.put(AlarmEnum.insulation.toString, 3)
            if (insulationAlarmCount3 > 4) {
              json.put("insulationAlarmCount3", 2)
            }
          } else if (insulationAlarmCount2 > 3) {
            json.put(AlarmEnum.insulation.toString, 2)
            if (insulationAlarmCount2 > 4) {
              json.put("insulationAlarmCount2", 2)
            }
          } else if (insulationAlarmCount1 > 2) {
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
        if (temperatureDiff >= 20) {
          json.put(AlarmEnum.temperatureDifferential.toString, 3) //温差报警temperatureDifferential
        } else if (temperatureDiff >= 18) {
          json.put(AlarmEnum.temperatureDifferential.toString, 2)
        } else if (temperatureDiff >= 15) {
          json.put(AlarmEnum.temperatureDifferential.toString, 1)
        }
      }
    }
  }

  /**
   * 电压采集(电压采集线脱落）
   *
   * @param json
   */
  def isVoltagelinefFall(json: JSONObject): Unit = {
    var cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val dcstatus: Integer = json.getInteger("dcStatus")
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.filter(_ == 0).length > 0) {
        if (cellVoltagesArray.filter(_ != 0).length > 0) {
          val avgCellVoltage = cellVoltagesArray.filter(_ != 0).sum.toDouble / cellVoltagesArray.filter(_ != 0).length
          if (avgCellVoltage >= 2000) {
            json.put(AlarmEnum.voltageLineFall.toString, 2)
            cellVoltagesArray = cellVoltagesArray.filter(_ != 0)
            json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
          }
        } else {
          //都为异常值的情况
          if (dcstatus == 0) {
            json.put(AlarmEnum.voltageLineFall.toString, 1)
            json.put("cellVoltages", null)
          } else {
            json.put(AlarmEnum.voltageLineFall.toString, 2)
            json.put("cellVoltages", null)
          }
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
    if (probeTemperaturesArray != null) {
      if (probeTemperaturesArray.filter(_ == 0).length > 0) {
        if (probeTemperaturesArray.filter(_ != 0).length > 0) {
          val avgTemperatures = probeTemperaturesArray.filter(_ != 0).sum.toDouble / probeTemperaturesArray.filter(_ != 0).length
          if (avgTemperatures >= 15) {
            json.put(AlarmEnum.tempLineFall.toString, 2)
            probeTemperaturesArray = probeTemperaturesArray.filter(_ != 0)
            json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
          }
        } else {
          json.put(AlarmEnum.tempLineFall.toString, 2)
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
    if (cellVoltagesArray != null) {
      if (cellVoltagesArray.filter(_ >= 4000).length > 0) {
        if (cellVoltagesArray.filter(_ < 4000).filter(_ != 0).length > 0) {
          val avgCellVoltages = cellVoltagesArray.filter(_ < 4000).filter(_ != 0).sum.toDouble / cellVoltagesArray.filter(_ < 4000).filter(_ != 0).length
          if (avgCellVoltages <= 3600) {
            json.put(AlarmEnum.abnormalVoltage.toString, 2)
            cellVoltagesArray = cellVoltagesArray.filter(_ < 4000)
            json.put("cellVoltages", JSON.toJSON(cellVoltagesArray))
          }
        } else {
          json.put(AlarmEnum.abnormalVoltage.toString, 2)
          json.put("cellVoltages", null)
        }
      }
    }
    else if (cellVoltagesArray == null)
      json.put(AlarmEnum.abnormalVoltage.toString, 1)
  }

  /**
   * 温度数据异常
   *
   * @param json
   */
  def isTempAbnormal(json: JSONObject): Unit = {
    var probeTemperaturesArray = stringToIntArray(json.getString("probeTemperatures"))
    if (probeTemperaturesArray != null) {
      if (probeTemperaturesArray.filter(_ >= 127).length > 0) {
        if (probeTemperaturesArray.filter(_ != 0).filter(_ < 127).length > 0) {
          val avgprobeTemperatures = probeTemperaturesArray.filter(_ != 0).filter(_ < 127).sum.toDouble / probeTemperaturesArray.filter(_ != 0).filter(_ < 127).length
          if (avgprobeTemperatures < 100) {
            json.put(AlarmEnum.abnormalTemperature.toString, 2)
            probeTemperaturesArray = probeTemperaturesArray.filter(_ < 127)
            json.put("probeTemperatures", JSON.toJSON(probeTemperaturesArray))
          }
        } else {
          json.put(AlarmEnum.abnormalTemperature.toString, 2)
          json.put("probeTemperatures", null)
          return
        }
      }
    } else {
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
      val maxIndex = cellVoltagesArray.zipWithIndex.max._2 //最大电压所在序号
      val delta = math.abs(max - min)

      if (max >= 3000 && min <= 3400 && delta > 180) { //差值过滤，排除大电流带来的影响
        //除去最后一个单体为最高电压的情况
        if (maxIndex != (cellVoltagesArray.length - 1)) {
          if (math.abs(max - cellVoltagesArray(maxIndex + 1)) >= 0.95 * delta) { //0.95是兼容多组故障的影响
            val avg = Math.abs(max + cellVoltagesArray(maxIndex + 1)) / 2
            if (cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).nonEmpty) {
              val avgCell = cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).sum / cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex + 1)).length //排除多组故障且最高电压相等的情况干扰
              if (math.abs(avg - avgCell) <= 50 && avg <= 3400 && avgCell <= 3400) { //排除动态干扰
                json.put(AlarmEnum.isAdjacentMonomerAbnormal.toString, 2)
                val isCellVoltageNeighborArray: Array[Int] = Array(avgCell, avgCell)
                val tempCellVoltagesArray = cellVoltagesArray.patch(maxIndex, isCellVoltageNeighborArray, 2)
                json.put("cellVoltages", JSON.toJSON(tempCellVoltagesArray))
                RecursiveCellVoltageNeighbor(json, tempCellVoltagesArray)
              }
            }
          }
        }
        //除去第一个单体为最高电压的情况
        if (maxIndex != 0) {
          if (math.abs(max - cellVoltagesArray(maxIndex - 1)) >= 0.95 * delta) {
            val avg = Math.abs(max + cellVoltagesArray(maxIndex - 1)) / 2
            if (cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).nonEmpty) {
              val avgCell = cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).sum / cellVoltagesArray.filter(_ != max).filter(_ != cellVoltagesArray(maxIndex - 1)).length
              if (math.abs(avg - avgCell) <= 50 && avg <= 3400 && avgCell <= 3400) { //奇瑞的充电末端一致性差
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
   * 内阻异常
   *
   * @param old_json
   * @param json
   * @return
   */


  def AbnormalinternalResistance(old_json: JSONObject, json: JSONObject) = {
    val chargeStatus: Int = json.getIntValue("chargeStatus")
    val current = json.getInteger("current")
    val old_current = old_json.getInteger("current")
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val old_cellVoltagesArray: Array[Int] = stringToIntArray(old_json.getString("cellVoltages"))

    val customField: String = json.getString("customField")
    val customFieldJson: JSONObject = JSON.parseObject(customField)
    neizuyichang(json, old_json)
    val count = json.getIntValue("count")
    if (cellVoltagesArray != null && cellVoltagesArray.nonEmpty && old_cellVoltagesArray != null && old_cellVoltagesArray.nonEmpty && count >= 30) {
      val maxIndex = cellVoltagesArray.zipWithIndex.max._2 //最大电压所在序号
      val old_maxIndex = old_cellVoltagesArray.zipWithIndex.max._2
      val minIndex = cellVoltagesArray.zipWithIndex.min._2 //最小电压所在序号
      val old_minIndex = old_cellVoltagesArray.zipWithIndex.min._2
      if (old_current > 0 && current < 0 && old_minIndex == maxIndex) {
        if (json.containsKey("yichangdu")) {
          val yichangduArray = stringToDoubleArray(json.getString("yichangdu"))
          if (math.abs(yichangduArray.apply(maxIndex)) > 3 && math.abs(yichangduArray.apply(maxIndex)) < 4) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 1)
          } else if (math.abs(yichangduArray.apply(maxIndex)) >= 4 && math.abs(yichangduArray.apply(maxIndex)) < 5) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 2)
          } else if (math.abs(yichangduArray.apply(maxIndex)) >= 5) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 3)
          }
          customFieldJson.put("index", maxIndex + 1)
          customFieldJson.put("yichangdu", JSON.toJSON(yichangduArray))
          json.put("customField", customFieldJson.toString)
        }

      }
      if (old_current < 0 && current > 0 && old_maxIndex == minIndex) {
        if (json.containsKey("yichangdu")) {
          val yichangduArray = stringToDoubleArray(json.getString("yichangdu"))
          if (math.abs(yichangduArray.apply(minIndex)) > 3 && math.abs(yichangduArray.apply(minIndex)) < 4) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 1)
          } else if (math.abs(yichangduArray.apply(minIndex)) >= 4 && math.abs(yichangduArray.apply(minIndex)) < 5) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 2)
          } else if (math.abs(yichangduArray.apply(minIndex)) >= 5) {
            json.put(AlarmEnum.isAbnormalinternalResistance.toString, 3)
          }
          customFieldJson.put("index", minIndex + 1)
          customFieldJson.put("yichangdu", JSON.toJSON(yichangduArray))
          json.put("customField", customFieldJson.toString)
        }

      }
    }


  }


  def neizuyichang(new_json: JSONObject, old_json: JSONObject) = {
    val chargeStatus:Int=new_json.getIntValue("chargeStatus")
    val old_current: Int = old_json.getIntValue("current")
    val current: Int = new_json.getIntValue("current")
    val time: Long = new_json.getLongValue("ctime")
    val old_time: Long = old_json.getLongValue("ctime")
    var RiArray: Array[Double] = Array()
    val cellVoltages = stringToIntArray(new_json.getString("cellVoltages"))
    val old_cellVoltages = stringToIntArray(old_json.getString("cellVoltages"))

    if (cellVoltages!=null && old_cellVoltages!=null && cellVoltages.nonEmpty && old_cellVoltages.nonEmpty && math.abs(current - old_current) > 5000 && time / 1000 - old_time / 1000 <= 10 && chargeStatus!=1 && chargeStatus<=4) {
      val count = old_json.getIntValue("count")
      new_json.put("count", count + 1)
      val delta_current = current - old_current
      RiArray = cellVoltages.zip(old_cellVoltages).map(x => (x._1 - x._2).toDouble / delta_current.toDouble)

      if (old_json.containsKey("dongtaisum_Ri")) {
        val old_sum_Ri = stringToDoubleArray(old_json.getString("dongtaisum_Ri"))
        val old_sumcurrent = old_json.getIntValue("sum_current")

        //内阻叠加
        val dongtaisum_Ri = RiArray.zip(old_sum_Ri).map { case (x, y) => x + y }
        new_json.put("dongtaisum_Ri", JSON.toJSON(dongtaisum_Ri))
        //电流叠加
        val sum_current = math.abs(delta_current) + math.abs(old_sumcurrent)
        new_json.put("sum_current", sum_current)
        if(count>=29){
          val zhiliuRidianliujiaquan_array = dongtaisum_Ri.map(x => (x * math.abs(current - old_current).toDouble) / sum_current.toDouble)

          val avg_zhiliuRidianliujiaquan = zhiliuRidianliujiaquan_array.sum / dongtaisum_Ri.length

          var variance: Double = 0
          variance = zhiliuRidianliujiaquan_array.map(x => math.pow(x - avg_zhiliuRidianliujiaquan, 2)).sum
          val std_diff = math.sqrt(variance / zhiliuRidianliujiaquan_array.length)
          val yichangduArray = zhiliuRidianliujiaquan_array.map(x => (x - avg_zhiliuRidianliujiaquan) / (std_diff + 0.00001))
          new_json.put("yichangdu", JSON.toJSON(yichangduArray))
        }

      } else {
        new_json.put("sum_current", math.abs(delta_current))
        new_json.put("dongtaisum_Ri", JSON.toJSON(RiArray))
      }

    } else {
      if (old_json.containsKey("dongtaisum_Ri")) {
        new_json.put("dongtaisum_Ri", JSON.toJSON(stringToDoubleArray(old_json.getString("dongtaisum_Ri"))))
        new_json.put("sum_current", old_json.getIntValue("sum_current"))
        new_json.put("count", old_json.getIntValue("count"))
      }

    }


  }
}
