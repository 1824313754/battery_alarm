package base

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.datastream.DataStream

trait FlinkBase extends Serializable {
  //定义一个处理方法，用于处理数据，入参为DataStream[JSONObject]，返回值为DataStream[JSONObject]
  def process(dataStream: DataStream[JSONObject]): DataStream[JSONObject]

}
