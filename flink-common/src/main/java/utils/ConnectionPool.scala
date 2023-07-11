package utils

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory

import java.sql.Connection

object ConnectionPool {
  //打印日志
  val logger = LoggerFactory.getLogger(ConnectionPool.getClass)
  var connectionPool:BoneCP=_
  def getConnection(properties:ParameterTool)={
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(properties.get("mysql.conn"))
      config.setUsername(properties.get("mysql.user"))
      config.setPassword(properties.get("mysql.passwd"))
      config.setMinConnectionsPerPartition(1)
      config.setMaxConnectionsPerPartition(2)
      config.setPartitionCount(6)
      config.setCloseConnectionWatch(false)
      config.setLazyInit(true)
      connectionPool = new BoneCP(config)
//      print("connectionPool: " + connectionPool)
      val connection = connectionPool.getConnection
      connection
    } catch {
      case exception: Exception => logger.error("create Connection Error: \n" + exception.getMessage)
        null
    }
  }
//  def getConnection:Option[Connection] = {
//    Option(connectionPool.getConnection)
//  }

  def closeConnection(connection: Connection)={
    if (!connection.isClosed){
      connection.close()
    }

  }

}
