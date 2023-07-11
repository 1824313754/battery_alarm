package sink

import bean.ClickhouseAlarmTable
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import utils.GetConfig
import utils.SqlUtils.sqlProducer


class ClickHouseSink(properties :ParameterTool) extends RichSinkFunction[ClickhouseAlarmTable] {
//  val properties = GetConfig.getProperties("test.properties")
  private var connection: ClickHouseConnection = _
  private val tableName: String = properties.get("clickhouse.table")
  override def open(parameters: Configuration): Unit = {
    val clickPro = new ClickHouseProperties()
    clickPro.setUser(properties.get("clickhouse.user"))
    clickPro.setPassword(properties.get("clickhouse.passwd"))
    val source = new BalancedClickhouseDataSource(properties.get("clickhouse.conn"), clickPro)
    source.actualize()
    connection = source.getConnection
  }

  override def invoke(alarmInfo: ClickhouseAlarmTable, context: Context): Unit = {
    val statement = connection.createStatement()
    val query: String = sqlProducer(tableName, alarmInfo)
    statement.executeUpdate(query)
  }

  override def close(): Unit = {
    connection.close()
  }
}

