package `enum`

/**
 * 枚举报警类型
 */
object AlarmEnum  extends  Enumeration{
  type AlarmEnum = Value

  val batteryHighTemperature=Value("batteryHighTemperature")//电池高温
  val socJump=Value("socJump")//soc跳变
  val socHigh=Value("socHigh")//soc虚高
  val monomerBatteryUnderVoltage=Value("monomerBatteryUnderVoltage")//单体电池欠压
  val monomerBatteryOverVoltage=Value("monomerBatteryOverVoltage")//单体电池过压
  val deviceTypeUnderVoltage=Value("deviceTypeUnderVoltage")//总电池欠压/储能装置欠压
  val deviceTypeOverVoltage=Value("deviceTypeOverVoltage")//总电池过压/储能装置过压
  val batteryConsistencyPoor=Value("batteryConsistencyPoor")//电池一致性差
  val insulation=Value("insulation")//绝缘故障
  val socLow=Value("socLow")//soc低
  val temperatureDifferential=Value("temperatureDifferential")//温度差异
  val voltageJump=Value("voltageJump")//电压跳变
  val socNotBalance=Value("socNotBalance")//soc不平衡
  val electricBoxWithWater=Value("electricBoxWithWater")//电箱进水
  val outFactorySafetyInspection=Value("outFactorySafetyInspection")//出场安全检查
  val abnormalTemperature=Value("abnormalTemperature")//温度异常
  val abnormalVoltage=Value("abnormalVoltage")//电压异常
  val voltageLineFall=Value("voltageLineFall")//电压采集线脱落
  val tempLineFall=Value("tempLineFall")//温度采集线脱落
  val isAdjacentMonomerAbnormal=Value("isAdjacentMonomerAbnormal")//相邻单体故障
  val batteryStaticConsistencyPoor=Value("batteryStaticConsistencyPoor")//静态压差
  val isAbnormalinternalResistance=Value("isAbnormalinternalResistance")//内阻异常
  val safeVoltageRise=Value("safeVoltageRise")//安全机理部电压策略
  val safeTemperatureChange=Value("safeTemperatureChange")//安全机理部温升策略1
  val safeTemperatureRise=Value("safeTemperatureRise")//安全机理部温升策略2
  val slaveDisconnect=Value("slaveDisconnect")//从机掉线
}
