package bean



import org.apache.flink.api.java.utils.ParameterTool
import utils.ConnectionPool

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DictConfig {
 @volatile private var instance: DictConfig = _

  def getInstance(properties: ParameterTool): DictConfig = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new DictConfig(properties)
        }
      }
    }
    instance
  }
}

class DictConfig private(properties: ParameterTool) {
  @volatile private var ocvTable: Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]] = _
  @volatile private var alarmDict: Map[String, String] = _
  @volatile private var vehicleFactoryDict: Map[Int, String] = _
  @volatile private var projectName: Map[String,(Int,String,String)] = _
  private def getProjectName(): Map[String,(Int,String,String)] = {
    val conn = ConnectionPool.getConnection(properties)
    val sql:String="select a.vin,a.ProID,b.`Code`,b.`Name` from (select vin,ProID from GX_Vin) a,(select id,`Code`,`Name` from GX_Project2) b where a.ProID=b.id "
    //设置参数
    val prepareStatement = conn.prepareStatement(sql)
    val result = prepareStatement.executeQuery()
    // 定义一个map，用于存放查询结果
    var projectNameMap = Map[String, (Int, String, String)]()
    while (result.next()){
      val vin = result.getString("vin")
      val proID = result.getInt("ProID")
      val code = result.getString("Code")
      val name = result.getString("Name")
      //vin码作为key,proID,code,name作为value
      projectNameMap+=(vin->(proID,code,name))
    }
    // 完成后关闭
    prepareStatement.close();
    ConnectionPool.closeConnection(conn)
    projectNameMap
  }
  def getProjectNameInstance(): Map[String,(Int,String,String)] = {
    if (projectName == null) {
      synchronized {
        if (projectName == null) {
          println("GetProjectName")
          projectName = getProjectName
        }
      }
    }
    projectName
  }
  private def getOcvNCMData(): Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]] = {
    val conn = ConnectionPool.getConnection(properties)
    val sql: String = "SELECT BatteryAh,BatteryTemp,BatteryCellVol,BatterySoc from battery.GX_Ocv order by id"
    val prepareStatement = conn.prepareStatement(sql)
    val result = prepareStatement.executeQuery()
    val map = mutable.Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]]()
    while (result.next()) {
      val batteryAh = result.getString("BatteryAh")
      val batteryTemp = result.getInt("BatteryTemp")
      val batteryCellVol = result.getInt("BatteryCellVol")
      val batterySoc = result.getFloat("BatterySoc")

      if (map.contains(batteryAh)) {
        val innerMap = map(batteryAh)
        if (innerMap.contains(batteryTemp)) {
          innerMap(batteryTemp).append((batteryCellVol, batterySoc))
        } else {
          innerMap += (batteryTemp -> ArrayBuffer((batteryCellVol, batterySoc)))
        }
      } else {
        val innerMap = mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]()
        innerMap += (batteryTemp -> ArrayBuffer((batteryCellVol, batterySoc)))
        map += (batteryAh -> innerMap)
      }
    }

    prepareStatement.close()
    ConnectionPool.closeConnection(conn)
    map.toMap
  }


  private def getAlarmDict(): Map[String, String] = {
    val conn = ConnectionPool.getConnection(properties)
    var map = Map[String, String]()
    val sql: String = "SELECT alarm_type,alarm_name from alarm_type"
    val prepareStatement = conn.prepareStatement(sql)
    val result = prepareStatement.executeQuery()
    while (result.next()) {
      val alarm_type = result.getString("alarm_type")
      val alarm_name = result.getString("alarm_name")
      map += (alarm_type -> alarm_name)
    }
    prepareStatement.close()
    ConnectionPool.closeConnection(conn)
    map
  }

  private def getVehicleFactoryDict(): Map[Int, String] = {
    val conn = ConnectionPool.getConnection(properties)
    var map = Map[Int, String]()
    val sql: String = "SELECT vehicle_factory_name,vehicle_factory_code from battery.t_vehicle_factory"
    val prepareStatement = conn.prepareStatement(sql)
    val result = prepareStatement.executeQuery()
    while (result.next()) {
      val vehicle_factory_name = result.getString("vehicle_factory_name")
      val vehicle_factory_code = result.getInt("vehicle_factory_code")
      map += (vehicle_factory_code -> vehicle_factory_name)
    }
    prepareStatement.close()
    ConnectionPool.closeConnection(conn)
    map
  }

  def creatInstanceNCM(): Map[String, mutable.TreeMap[Int, ArrayBuffer[(Int, Float)]]] = {
    if (ocvTable == null ) {
      synchronized {
        if (ocvTable == null) {
          println("creatInstanceNCM")
          ocvTable = getOcvNCMData()
        }
      }
    }
    ocvTable
  }

  def getAlarmDictInstance(): Map[String, String] = {
    if (alarmDict == null) {
      synchronized {
        if (alarmDict == null) {
          println("getAlarmDictInstance")
          alarmDict = getAlarmDict()
        }
      }
    }
    alarmDict
  }

  def getVehicleFactoryDictInstance(): Map[Int, String] = {
    if (vehicleFactoryDict == null) {
      synchronized {
        if (vehicleFactoryDict == null) {
          println("getVehicleFactoryDictInstance")
          vehicleFactoryDict = getVehicleFactoryDict()
        }
      }
    }
    vehicleFactoryDict
  }
}