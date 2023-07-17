package coom

/**
 * 吉利自唤醒电压分层处理
 */
object VoltageFuncs {
  def processVoltageLayer(old_cellVoltages: Array[Int], cellVoltages: Array[Int]): Array[Int] = {
    var modifiedArray: Array[Int] = null
    if (old_cellVoltages != null && old_cellVoltages.length == cellVoltages.length) {
      val Vmin: Int = cellVoltages.min
      val Vmax: Int = cellVoltages.max
      val minVmid: Int = old_cellVoltages(cellVoltages.indexOf(Vmin))
      val maxVmid: Int = old_cellVoltages(cellVoltages.indexOf(Vmax))

      if (0 <= (Vmin - minVmid) && (Vmin - minVmid) <= 15) {
        modifiedArray = voltageUp(old_cellVoltages, cellVoltages)
      } else if (0 <= (maxVmid - Vmax) && (maxVmid - Vmax) <= 12) {
        modifiedArray = voltageDown(old_cellVoltages, cellVoltages)
      }
    }
    modifiedArray
  }

  // TODO: 电压回升
  def voltageUp(old_cellVoltages: Array[Int], cellVoltages: Array[Int]): Array[Int] = {
    val Vmin: Int = cellVoltages.min
    val indexMin: Int = cellVoltages.indexOf(Vmin)
    val Vmid: Int = old_cellVoltages(indexMin)
    val diff = Vmin - Vmid

    // 判断相邻有无分层
    if (indexMin == 0) {
      if ((cellVoltages(indexMin + 1) - old_cellVoltages(indexMin + 1)) > (diff + 1)) {
        return null
      }
    } else if (indexMin == (cellVoltages.length - 1)) {
      if ((cellVoltages(indexMin - 1) - old_cellVoltages(indexMin - 1)) > (diff + 1)) {
        return null
      }
    } else {
      if ((cellVoltages(indexMin + 1) - old_cellVoltages(indexMin + 1)) > (diff + 1)
        && (cellVoltages(indexMin - 1) - old_cellVoltages(indexMin - 1)) > (diff + 1)) {
        return null
      }
    }

    // 遍历单体分别比较
    var count1: Int = 0
    var count2: Int = 0
    var normalSum: Int = 0
    for (i <- 0 until cellVoltages.length) {
      val diffVoltage: Int = cellVoltages(i) - old_cellVoltages(i)
      if (diffVoltage >= 0 && diffVoltage <= (diff + 2)) {
        count1 = count1 + 1
      } else if (diffVoltage > (20 + 2 * diff)) {
        count2 = count2 + 1
        normalSum = normalSum + cellVoltages(i)
      }
    }
    if (count2 > 10 && count1 >= 3 && (count1 + count2) == cellVoltages.length) {
      for (i <- 0 until cellVoltages.length) {
        val diffVoltage: Int = cellVoltages(i) - old_cellVoltages(i)
        if (diffVoltage >= 0 && diffVoltage <= (diff + 2)) {
          cellVoltages(i) = normalSum / count2 // 用正常电压替换异常电压
        }
      }
      return cellVoltages
    }
    null
  }

  // TODO: 电压回落
  def voltageDown(old_cellVoltages: Array[Int], cellVoltages: Array[Int]): Array[Int] = {
    val Vmax: Int = cellVoltages.max
    val indexMax: Int = cellVoltages.indexOf(Vmax)
    val Vmid: Int = old_cellVoltages(indexMax)
    val diff = Vmid - Vmax

    // 判断相邻是否分层
    if (indexMax == 0) {
      if ((old_cellVoltages(indexMax + 1) - cellVoltages(indexMax + 1)) > (diff + 1)) {
        return null
      }
    } else if (indexMax == (cellVoltages.length - 1)) {
      if ((old_cellVoltages(indexMax - 1) - cellVoltages(indexMax - 1)) > (diff + 1)) {
        return null
      }
    } else {
      if ((old_cellVoltages(indexMax + 1) - cellVoltages(indexMax + 1)) > (diff + 1)
        && (old_cellVoltages(indexMax - 1) - cellVoltages(indexMax - 1)) > (diff + 1)) {
        return null
      }
    }

    // 遍历单体分别比较
    var count1: Int = 0
    var count2: Int = 0
    var normalSum: Int = 0
    for (i <- 0 until cellVoltages.length) {
      val diffVoltage: Int = old_cellVoltages(i) - cellVoltages(i)
      if (diffVoltage >= 0 && diffVoltage <= (diff + 4)) {
        count1 = count1 + 1
      } else if (diffVoltage > (30 + 2 * diff)) {
        count2 = count2 + 1
        normalSum = normalSum + cellVoltages(i)
      }
    }
    if (count2 > 10 && count1 >= 3 && (count1 + count2) == cellVoltages.length) {
      for (i <- 0 until cellVoltages.length) {
        val diffVoltage: Int = old_cellVoltages(i) - cellVoltages(i)
        if (diffVoltage >= 0 && diffVoltage <= (diff + 4)) {
          cellVoltages(i) = normalSum / count2 // 用正常电压替换异常电压
        }
      }
      return cellVoltages
    }
    null
  }
}
