package utils

import java.text.SimpleDateFormat

object CommonFuncs {

  /**
    *
    * @param str  ["1","2","3]
    * @return      Array[1,2,3]
    */
  def stringToIntArray(str:String):Array[Int]={

    if(str!=null && str.length>2) {
      val strArr: Array[String] = str.substring(1, str.length - 1).split(",")
      val intArr = new Array[Int](strArr.length);
      for (i <- 0 until intArr.length) {
        intArr(i) = strArr(i).toInt
      }
      return  intArr
    }
    null
  }

  /*
  *日期转时间戳
   */
  def getTimestamp(x : String) : Long  = {
    val format = new SimpleDateFormat("yyyyMMdd")
    format.parse(x).getTime/1000
  }

  def mkctime (year:Int,month:Int,day:Int,hours:Int,minutes:Int,seconds:Int) :Long ={
    //println("year:"+year+",month:"+month+",day:"+day+",hours:"+hours+",minutes:"+minutes+",seconds"+seconds);
    try {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("20%02d-%02d-%02d %02d:%02d:%02d".format(year, month, day, hours, minutes, seconds)).getTime / 1000
    }catch {
      case e:Throwable=> return 0;
    }
  }



  /*
    *获取经纬度RDD
     */
  def locateCityRDD(lon :Double ,lat :Double,gps_rdd :Array[String]) :String={
//    println(s"-------${gps_rdd.length}")
    val current_place = gps_rdd.map(rdd =>{
      val splits = rdd.split(",")
      val llat = splits(3).toDouble
      val llon = splits(4).toDouble
      (splits(0),splits(1),splits(2),splits(5),(llat - lat) * (llat - lat) + (llon - lon) * (llon - lon))
    }).sortBy(x => x._5)
      .take(1)
    current_place.mkString(",")
  }
  def getTimeStr(): String = {
    // 获取当前时间
    val now = System.currentTimeMillis()
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(now)
  }

  /**
   * 秒级时间戳转为日期yyyy-MM-dd HH:mm:ss
   * @param timestamp
   */
  def timestampToDate(timestamp: Long): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(timestamp * 1000)
  }

  def stringToDoubleArray(str: String): Array[Double] = {
    if (str != null && str.length > 2) {
      val strArr: Array[String] = str.substring(1, str.length - 1).split(",")
      var doubleArr = new Array[Double](strArr.length);
      doubleArr = strArr.map(x => x.toDouble)

      doubleArr
    } else {
      null
    }
  }



}
