package coom

import com.alibaba.fastjson.JSONObject
import utils.CommonFuncs.stringToIntArray

object AdvancedFuncs {
  /**
   * 铁锂：与最小电压相近单体达一定数量 => 电压分层 => 两帧报警
   */
  def notSocLayerLFP(old_json: JSONObject, json: JSONObject): Boolean = {
    val cellVoltagesArray = stringToIntArray(json.getString("cellVoltages"))
    val minCellVoltage = cellVoltagesArray.min
    val maxCellVoltage = cellVoltagesArray.max

    if (!old_json.containsKey("isSocLayer")) {
      var count1 = 0
      var count2 = 0
      for (i <- 0 until cellVoltagesArray.length) {
        if (cellVoltagesArray(i) <= (minCellVoltage + (maxCellVoltage - minCellVoltage) * 0.1 + 10)) {
          count1 = count1 + 1
          count2 = count2 + 1
        } else {
          count2 = 0
        }
        if (count1 >= 5 || count2 >= 3) {
          json.put("isSocLayer", 1)
          return false
        }
      }
    }
    true
  }
}
