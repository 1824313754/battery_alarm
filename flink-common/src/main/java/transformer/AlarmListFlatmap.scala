package transformer

import `enum`.AlarmEnum
import bean.ClickhouseAlarmTable
import bean.base.ConfigParams
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.CommonFuncs

import java.util.UUID

class AlarmListFlatmap extends RichFlatMapFunction[JSONObject, ClickhouseAlarmTable] {
  //定义一个报警等级的map
  val alarmLevelMap=Map(
    1->"一级报警",
    2->"二级报警",
    3->"三级报警",
    4->"四级报警"
  )
  //定义列表状态，用于存储报警类型
  var alarmTypeList:Map[String,String] = _
  override def open(parameters: Configuration): Unit = {
    //获取全局变量
    val properties = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    alarmTypeList=new ConfigParams(properties).getAlarmDictInstance()
  }

  override def flatMap(json: JSONObject, out: Collector[ClickhouseAlarmTable]): Unit = {
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
        json.put("day_of_year", CommonFuncs.timestampToDate(json.getLongValue("timeStamp")).substring(0,10))
        json.put("vehicle_factory",json.getString("vehicleFactory"))
        json.put("uuid",UUID.randomUUID().toString)
        //将json映射为AlarmCount对象
        val alarmCount: ClickhouseAlarmTable = JSON.parseObject(json.toJSONString, classOf[ClickhouseAlarmTable])
        //返回对象流
        out.collect(alarmCount)
      }
    }
  }
}
