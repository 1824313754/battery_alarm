package transformer

import com.alibaba.fastjson.{JSONObject}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import java.nio.charset.StandardCharsets

/**
 * gps处理
 */
class GpsProcess extends RichMapFunction[JSONObject, String] {
  var gpsList = Array[String]()
  override def open(parameters: Configuration): Unit = {
    //读取gpsPath中的数据
    val gps = getRuntimeContext.getDistributedCache.getFile("gps")
    gpsList = scala.io.Source.fromFile(gps, StandardCharsets.UTF_8.toString).getLines().toArray
  }


  override def map(value: JSONObject): String = {
    val cityStr = parseCity(value, gpsList)
    val cityArray: Array[String] = cityStr.split(",")
    value.put("province", cityArray(0))
    value.put("city", cityArray(1))
    value.put("area", cityArray(2))
    value.put("region", cityArray(3))
    value.toJSONString
  }


  def parseCity(jsonobject: JSONObject, gpsList: Array[String]): String = {
    val lon = jsonobject.getDouble("longitude")
    val lat = jsonobject.getDouble("latitude")
    var locate = " , , , "
    if (lon != null && lat != null && lon != 0 && lat != 0) {
      locate = locateCityRDD(lon / 1000000, lat / 1000000, gpsList)
    }
    locate
  }

  /*
      *获取经纬度
       */
  def locateCityRDD(lon: Double, lat: Double, gps_rdd: Array[String]): String = {
    //    println(s"-------${gps_rdd.length}")
    val current_place = gps_rdd.map(rdd => {
      val splits = rdd.split(",")
      val llat = splits(3).toDouble
      val llon = splits(4).toDouble
      (splits(0), splits(1), splits(2), splits(5), (llat - lat) * (llat - lat) + (llon - lon) * (llon - lon))
    }).sortBy(x => x._5)
      .take(1)
    current_place.mkString(",")
  }
}
