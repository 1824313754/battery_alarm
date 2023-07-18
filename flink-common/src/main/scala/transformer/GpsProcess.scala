package transformer

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration


import scala.collection.mutable

class GpsProcess extends RichMapFunction[JSONObject, JSONObject]  {
  var locations: List[Location] = _
  override def open(parameters: Configuration): Unit = {
    //读取gpsPath中的数据
    val gps = getRuntimeContext.getDistributedCache.getFile("gps")
    val gpsList: List[String] = scala.io.Source.fromFile(gps).getLines().toList
    locations = parseLocations(gpsList)
  }

  override def map(value: JSONObject): JSONObject = {
    val lon = value.getDouble("longitude")/1000000
    val lat = value.getDouble("latitude")/1000000
    val location: Option[Location] = locateProvinceCityDistrict(lat, lon, locations)
    value.put("province",location.get.province)
    value.put("city",location.get.city)
    value.put("area",location.get.district)
//    value.put("region",location.get.province+location.get.city+location.get.district)
    value
  }


  // 解析位置信息列表
  def parseLocations(locationList: List[String]): List[Location] = {
    locationList.map { locationString =>
      val parts = locationString.split(",")
      val province = parts(0).replace("i18n_", "")
      val city = parts(1).replace("i18n_", "")
      val district = parts(2).replace("i18n_", "")
      val latitude = parts(3).toDouble
      val longitude = parts(4).toDouble
      Location(province, city, district, latitude, longitude)
    }
  }

  // 计算两个位置之间的距离（使用简化的经纬度计算公式）
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val earthRadius = 6371.0 // 地球半径（单位：公里）
    val dLat = Math.toRadians(lat2 - lat1)
    val dLon = Math.toRadians(lon2 - lon1)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    earthRadius * c
  }
  // 根据经纬度定位到省市区
  def locateProvinceCityDistrict(latitude: Double, longitude: Double, locations: List[Location]): Option[Location] = {
    // 构建一个可变列表来存储匹配的位置信息和对应的距离
    val distances = mutable.ListBuffer[(Location, Double)]()

    // 计算输入位置与每个位置信息的距离，并存储到 distances 列表中
    for (location <- locations) {
      val distance = calculateDistance(latitude, longitude, location.latitude, location.longitude)
      distances.append((location, distance))
    }
    // 按距离从小到大排序
    val sortedDistances = distances.sortBy(_._2)
    // 获取距离最小的位置信息
    val closestLocation = sortedDistances.headOption.map(_._1)

    closestLocation
  }
  // 创建一个类来表示位置信息
  case class Location(province: String, city: String, district: String, latitude: Double, longitude: Double)
}
