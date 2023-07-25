package sink

import bean.ClickhouseAlarmTable
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import utils.RedisUtil
import utils.SqlUtils.sqlProducer

/**
 * 写入clickhouse，失败可重连
 * @param properties
 * @param <T>
 *
 */

class ClickHouseSink(properties: ParameterTool) extends RichSinkFunction[String] {
  private var connection: ClickHouseConnection = _
  private val tableName: String = properties.get("clickhouse.table")
  //重试次数
  private val maxRetries: Int = properties.getInt("clickhouse.maxRetries", 3)
//  private var redisUtil: RedisUtil = _
  override def open(parameters: Configuration): Unit = {
    connect()
  }

  override def invoke(alarmInfo: String, context: Context): Unit = {
    var retries = 0
    var success = false
    val alarm =JSON.parseObject(alarmInfo).toJavaObject(classOf[ClickhouseAlarmTable])
    while (!success && retries < maxRetries) {
      try {
        val statement = connection.createStatement()
        val query: String = sqlProducer(tableName,alarm)
        statement.executeUpdate(query)
        success = true
      } catch {
        case e: Exception =>
          retries += 1
          if (retries < maxRetries) {
            println(s"Writing record failed, retrying ($retries/$maxRetries)." + e.getMessage)
            connect() // 重连
          } else {
            println(s"Max retries exceeded. Failed to write to ClickHouse.")
            e.printStackTrace()
          }
      }
    }
  }

  override def close(): Unit = {
    connection.close()
  }

  private def connect(): Unit = {
    var connected = false
    var retries = 0
    while (!connected && retries < maxRetries) {
      try {
        val clickPro = new ClickHouseProperties()
        clickPro.setUser(properties.get("clickhouse.user"))
        clickPro.setPassword(properties.get("clickhouse.passwd"))
        val source = new BalancedClickhouseDataSource(properties.get("clickhouse.conn"), clickPro)
        source.actualize()
        connection = source.getConnection
        connected = true
      } catch {
        case e: Exception =>
          retries += 1
          if (retries < maxRetries) {
            println(s"Failed to connect to ClickHouse, retrying ($retries/$maxRetries)." + e.getMessage)
          } else {
            println(s"Max retries exceeded. Failed to connect to ClickHouse.")
            e.printStackTrace()
          }
      }
    }

  }
  }