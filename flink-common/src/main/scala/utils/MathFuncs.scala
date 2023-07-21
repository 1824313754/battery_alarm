package utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MathFuncs extends Serializable{
  /**
   * 线性插值
   *
   * @param x0
   * @param y0
   * @param x1
   * @param y1
   * @param x
   * @return
   */
  def linearInsertion(x0: Double, y0: Double, x1: Double, y1: Double, x: Int): Double = {
    // println("("+x0+","+y0+")--"+x1+","+y1+")");

    return (x - x1) / (x0 - x1) * y0 + (x - x0) / (x1 - x0) * y1
  }


  /**
   * 查表计算SOC值
   *
   * @param temperature
   * @param cellVoltage
   * @return
   */
  def calcJacSoc(temperature: Integer, cellVoltage: Integer): Double = {


    0
  }

  def getTemperatureIndex(temperature: Double): Integer = {

    if (temperature > 55)
      return 0;
    if (temperature < -30)
      return 17;

    var x = 0;
    if (temperature >= 0) {
      val i: Int = temperature.toInt / 5
      val j: Double = temperature % 5
      if (j <= 2.5) {
        x = 11 - i;
      } else {
        x = 11 - (i + 1);
      }
    } else {
      val posTemperature: Double = math.abs(temperature)
      val i: Int = posTemperature.toInt / 5
      val j: Double = posTemperature % 5
      if (j <= 2.5) {
        x = 11 + i;
      } else {
        x = 11 + i + 1;
      }
    }
    x
  }


  def getTemperatureIndex(temperature: Integer): Integer = {

    if (temperature > 55)
      return 0;
    if (temperature < -30)
      return 17;

    var x = 0;
    if (temperature >= 0) {
      val i: Int = temperature.toInt / 5
      val j: Double = temperature % 5
      if (j <= 2.5) {
        x = 11 - i;
      } else {
        x = 11 - (i + 1);
      }
    } else {
      val posTemperature: Double = math.abs(temperature)
      val i: Int = posTemperature.toInt / 5
      val j: Double = posTemperature % 5
      if (j <= 2.5) {
        x = 11 + i;
      } else {
        x = 11 + i + 1;
      }
    }
    x
  }


  /**
   * 计算吉利SOC
   *
   * @param cellVoltage
   * @param temperature
   * @return
   */
  def calcSoc(map: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]], cellVoltage: Int, temperature: Int): Int = {
    //通过计算插值获取数组中元素
//    var map: TreeMap[Int, ArrayBuffer[(Int, Float)]] = null
    var soc: Int = 0
//    if (index == 0) {
//      map = OcvData.creatInstanceNCM()
//    } else if (index == 1) {
//      map = OcvData.creatInstanceLFP_DJ2136()
//    } else if (index == 2) {
//      map = OcvData.creatInstanceLFP_DJ2137()
//    }

    if (map != null && !map.contains(temperature)) {
      map += (temperature -> ArrayBuffer())
      val firstKey = map.filterKeys(_ < temperature).lastKey
      val lastKey = map.filterKeys(_ > temperature).firstKey
      val firstArray = map(firstKey)
      val lastArray = map(lastKey)
      //计算温度插值
      for (i <- 0 until firstArray.length) {
        val d: Double = linearInsertion(firstKey, firstArray(i)._1, lastKey, lastArray(i)._1, temperature)
        map(temperature).append((d.toInt, firstArray(i)._2))
      }
      //  for((k,v)<-map) println("index(1):",index,(k,v))
      val tempArray = map(temperature)
      if (tempArray.exists(_._1 == cellVoltage)) {
        soc = tempArray.filter(_._1 == cellVoltage).head._2.toInt
      } else {
        var head: (Int, Float) = (cellVoltage, 0)
        var last: (Int, Float) = (cellVoltage, 100)
        if (tempArray.exists(_._1 < cellVoltage)) {
          val tuples = tempArray.filter(_._1 < cellVoltage).sortBy(_._1) //默认升序
          head = tuples.last
        } else
          return 0

        if (tempArray.exists(_._1 > cellVoltage)) {
          val tuples1 = tempArray.filter(_._1 > cellVoltage).sortBy(_._1) //默认升序
          last = tuples1.head
        } else
          return 100
        soc = linearInsertion(head._1, head._2, last._1, last._2, cellVoltage).toInt

      }
    } else if (map != null) {
      val socArray: ArrayBuffer[(Int, Float)] = map(temperature)
      if (socArray.exists(_._1 == cellVoltage)) {
        soc = socArray.filter(_._1 == cellVoltage).head._2.toInt
      } else {
        var head: (Int, Float) = (cellVoltage, 0)
        var last: (Int, Float) = (cellVoltage, 100)
        if (socArray.exists(_._1 < cellVoltage)) {
          val tuples = socArray.filter(_._1 < cellVoltage).sortBy(_._1) //默认升序
          head = tuples.last
        } else
          return 0

        if (socArray.exists(_._1 > cellVoltage)) {
          val tuples1 = socArray.filter(_._1 > cellVoltage).sortBy(_._1) //默认升序
          last = tuples1.head
        } else
          return 100
        soc = linearInsertion(head._1, head._2, last._1, last._2, cellVoltage).toInt
      }
    }
    soc
  }
}