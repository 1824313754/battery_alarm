package bean

package base

import org.apache.flink.api.java.utils.ParameterTool
import utils.ConnectionPool

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
class ConfigParams(properties: ParameterTool) {
  @volatile private var ocvTable: mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]] = null
  @volatile private var alarmDict: Map[String, String] = _
  private def getOcvNCMData(name:String):mutable.TreeMap[Int,ArrayBuffer[(Int,Float)]]={
    //初始化数据库连接
    val conn = ConnectionPool.getConnection(properties)
    val sql:String="SELECT BatteryTemp,BatteryCellVol,BatterySoc from battery.GX_Ocv where BatteryAh =?  order by id"
    val prepareStatement = conn.prepareStatement(sql)
    prepareStatement.setString(1,name)
//    prepareStatement.setInt(2,-20)
    val result = prepareStatement.executeQuery()
    var map = mutable.TreeMap[Int,ArrayBuffer[(Int,Float)]]()
    while (result.next()){
      val batteryTemp = result.getInt("BatteryTemp")
      val batteryCellVol = result.getInt("BatteryCellVol")
      val batterySoc = result.getFloat("BatterySoc")
      if(map.contains(batteryTemp)){ //判断key是否存在
        map(batteryTemp).append((batteryCellVol,batterySoc))
      }else{
        map+=(batteryTemp->ArrayBuffer((batteryCellVol,batterySoc)))
      }
    }
    // 完成后关闭
    prepareStatement.close();
    ConnectionPool.closeConnection(conn)
    map
  }

  private def  getAlarmDict(): Map[String, String]= {
    val conn = ConnectionPool.getConnection(properties)
    var map = Map[String,String]()
    val sql:String="SELECT alarm_type,alarm_name from alarm_type"
    val prepareStatement = conn.prepareStatement(sql)
    val result = prepareStatement.executeQuery()
    while (result.next()){
      val alarm_type = result.getString("alarm_type")
      val alarm_name = result.getString("alarm_name")
      map+=(alarm_type->alarm_name)
    }
    // 完成后关闭
    prepareStatement.close();
    ConnectionPool.closeConnection(conn)
    map
  }

  /**
   *
   * @return
   */
  def creatInstanceNCM(name:String)    = {
    if (ocvTable == null) {
      synchronized {
        println("creatInstanceNCM----GetOcvList-----")
        if (ocvTable == null) {
          ocvTable = getOcvNCMData(name)
        }
      }
    }
    ocvTable
  }
  def getAlarmDictInstance(): Map[String, String] = {
    if (alarmDict == null) {
      synchronized {
        if (alarmDict == null) {
          println("GetAlarmDict")
          alarmDict = getAlarmDict
        }
      }
    }
    alarmDict
  }

}

