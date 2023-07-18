package utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.flink.api.java.utils.ParameterTool
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool


object RedisUtil extends Serializable {
  @volatile private var instance:RedisUtil = _
  def getInstance(properties: ParameterTool): RedisUtil = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new RedisUtil(properties)
          instance.init()
        }
      }
    }
    instance
  }
}

class RedisUtil private(properties: ParameterTool) extends Serializable {
  private var jedisPool: Pool[Jedis] = _

  private def init(): Unit = {
    val config = new GenericObjectPoolConfig()
    config.setMaxTotal(properties.get("redis.maxCon").toInt)
    config.setMaxIdle(properties.get("redis.maxIdle").toInt)
    jedisPool = new JedisPool(config, properties.get("redis.host"),
      properties.get("redis.port").toInt,
      properties.get("redis.timeOut").toInt,
      properties.get("redis.password"), 0)
    println("connect redis successful!")
  }

  def setKey(key: String, value: String, database: Int): Boolean = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      jedis.select(database)
      jedis.set(key, value)
      true
    } catch {
      case e: Exception =>
        false
    } finally {
      if (jedis != null)
        jedis.close()
    }
  }

  def getKey(key: String, database: Int): String = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      jedis.select(database)
      jedis.get(key)
    } catch {
      case e: Exception =>
        null
    } finally {
      if (jedis != null)
        jedis.close()
    }
  }

}

