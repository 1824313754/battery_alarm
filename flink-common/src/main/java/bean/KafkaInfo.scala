package bean

import scala.beans.BeanProperty

class KafkaInfo extends Serializable{
  //topic
  @BeanProperty var topic: String = _
  //offset
  @BeanProperty var offset: Long = _
  //partition
  @BeanProperty var partition: Int = _
  //groupId
  @BeanProperty var groupId: String = _

}
