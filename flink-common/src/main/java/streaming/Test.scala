//package streaming
//
//import org.apache.flink.api.java.utils.ParameterTool
//import utils.RedisUtil
//
//object Test {
//  def main(args: Array[String]): Unit = {
//
//    //多线程创建RedisUtil，10个线程
//    for (i <- 1 to 10) {
//      new Thread(new Runnable {
//        override def run(): Unit = {
//          val jedis = RedisUtil.getInstance(ParameterTool.fromPropertiesFile("F:\\battery\\alarm\\test.properties"))
//          println(jedis)
//        }
//      }).start()
//    }
//  }
//}
