package transformer

import `enum`.AlarmEnum
import bean.{ConfigParams}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.CommonFuncs
import utils.CommonFuncs.stringToIntArray

import java.util.UUID

class AlarmListFlatmap extends RichFlatMapFunction[JSONObject, JSONObject] {
  //定义一个报警等级的map
  val alarmLevelMap=Map(
    1->"一级报警",
    2->"二级报警",
    3->"三级报警",
    4->"四级报警"
  )
  //定义列表状态，用于存储报警类型
  var alarmTypeList:Map[String,String] = _
  //定义车厂号和车厂名称的map
  var vehicleFactoryMap:Map[Int,String] = _
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val params = ConfigParams.getInstance(properties)
    alarmTypeList=params.getAlarmDictInstance()
    vehicleFactoryMap=params.getVehicleFactoryDictInstance()
  }

  override def flatMap(json: JSONObject, out: Collector[JSONObject]): Unit = {
    for (alarmType <- AlarmEnum.values) {
      if (json.containsKey(alarmType.toString) && json.getIntValue(alarmType.toString) > 0) {
        json.put("alarm_type",alarmType.toString)//报警类型
        json.put("alarm_level",json.getIntValue(alarmType.toString))//报警等级
        json.put("commandType",JSON.parseObject(json.getString("customField")).getIntValue("commandType"))
        //获取报警类型对应的中文名
        json.put("alarm_name", alarmTypeList.get(alarmType.toString).getOrElse(null))
        json.put("level_name", alarmLevelMap.get(json.getIntValue(alarmType.toString)).getOrElse(null)) //报警等级名称
        json.put("alarm_level", json.getIntValue(alarmType.toString)) //报警等级
        //添加报警次数和报警时间
        json.put("alarm_count", 1)
        json.put("start_time", json.getLongValue("timeStamp"))
        json.put("alarm_time",CommonFuncs.timestampToDate(json.getLongValue("timeStamp"))) //采集点时间
        json.put("day_of_year", CommonFuncs.timestampToDate(json.getLongValue("timeStamp")).substring(0,10))
        json.put("vehicle_factory",json.getString("vehicleFactory"))
        json.put("vehicle_factory_name",vehicleFactoryMap.get(json.getIntValue("vehicleFactory")).getOrElse(null))
        json.put("uuid",UUID.randomUUID().toString)
        json.put("process_time",CommonFuncs.getTimeStr())//报警开始时间

        //取出电压和温度数组计算平均值
        val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
        val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
        //计算平均值,若cellVoltagesArray为空或null则返回0
        if(cellVoltagesArray == null || cellVoltagesArray.length == 0 ){
          json.put("avgVoltage",0)
        }else{
          val cellVoltagesArrayLength:Float = cellVoltagesArray.length
          var avgVoltage: Float =cellVoltagesArray.sum / cellVoltagesArrayLength
          val formatted = f"$avgVoltage%.4f"
          avgVoltage = formatted.replaceAll("0*$", "").replaceAll("\\.$", "").toFloat
          json.put("avgVoltage",avgVoltage)
        }
        if (probeTemperaturesArray == null || probeTemperaturesArray.length == 0){
          json.put("avgTemperature",0)
        }else{
          val probeTemperaturesLength:Float = probeTemperaturesArray.length
          var avgTemperature: Float =probeTemperaturesArray.sum / probeTemperaturesLength-40
          val formatted = f"$avgTemperature%.1f"
          avgTemperature = formatted.replaceAll("0*$", "").replaceAll("\\.$", "").toFloat
          json.put("avgTemperature",avgTemperature)
        }
        val customField: JSONObject = json.getJSONObject("customField")
        val newObject = new JSONObject()
        //删除customField中其他的key，只保留commandType
        if (customField!=null) {
          val commandType: Int = customField.getIntValue("commandType")
          newObject.put("commandType",commandType)
          val index:Int=customField.getIntValue("index")
          val yichangdu=customField.getString("yichangdu")
          newObject.put("index",index)
          newObject.put("yichangdu",yichangdu)
          json.put("customField", newObject.toString)
        }

        //将json映射为AlarmCount对象
//        val alarmCount: JSONObject = JSON.parseObject(json.toJSONString, classOf[JSONObject])
        //返回对象流
        out.collect(json)
      }
    }
  }
}
