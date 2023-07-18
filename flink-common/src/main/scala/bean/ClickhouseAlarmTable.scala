package bean

import scala.beans.BeanProperty

class ClickhouseAlarmTable() extends  Serializable {
  @BeanProperty var uuid: String = _
  @BeanProperty var vin: String = _
  @BeanProperty var alarm_time: String = _
  @BeanProperty var alarm_type: String = _
  @BeanProperty var process_time: String = _
  @BeanProperty var city: String = _
  @BeanProperty var province: String = _
  @BeanProperty var area: String = _
  @BeanProperty var region: String = _
  @BeanProperty var level: Int = _
  @BeanProperty var status: Int = _
  @BeanProperty var vehicle_factory: Int = _
  @BeanProperty var chargeStatus: Int = _
  @BeanProperty var mileage: Long = _
  @BeanProperty var voltage: Int = _
  @BeanProperty var current: Int = _
  @BeanProperty var soc: Int = _
  @BeanProperty var dcStatus: Int = _
  @BeanProperty var insulationResistance: Int = _
  @BeanProperty var maxVoltageSystemNum: Int = _
  @BeanProperty var maxVoltagebatteryNum: Int = _
  @BeanProperty var batteryMaxVoltage: Int = _
  @BeanProperty var minVoltageSystemNum: Int = _
  @BeanProperty var minVoltagebatteryNum: Int = _
  @BeanProperty var batteryMinVoltage: Int = _
  @BeanProperty var maxTemperatureSystemNum: Int = _
  @BeanProperty var maxTemperatureNum: Int = _
  @BeanProperty var maxTemperature: Int = _
  @BeanProperty var minTemperatureSystemNum: Int = _
  @BeanProperty var minTemperatureNum: Int = _
  @BeanProperty var minTemperature: Int = _
  @BeanProperty var temperatureProbeCount: Int = _
  @BeanProperty var cellCount: Int = _
  @BeanProperty var soc_diff_value: Double = _
  @BeanProperty var temperature_diff: Int = _
  @BeanProperty var longitude: Long = _
  @BeanProperty var latitude: Long = _
  @BeanProperty var speed: Int = _
  @BeanProperty var day_of_year: String = _
  @BeanProperty var alarm_name: String = _
  @BeanProperty var level_name: String = _
  @BeanProperty var vehicle_factory_name: String = _
  @BeanProperty var avgVoltage: Float = _
  @BeanProperty var avgTemperature: Float = _
  @BeanProperty var project_id: Int = _
  @BeanProperty var project_code: String = _
  @BeanProperty var project_name: String = _
  @BeanProperty var customField: String = _

}