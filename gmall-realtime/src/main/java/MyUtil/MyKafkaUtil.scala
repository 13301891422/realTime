package MyUtil

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * @Description:
 * @author oranglzc
 * @creat 2020-07-14-19:20
 */
object MyKafkaUtil {
  val kafkaParams:Map[String,Object]=Map[String,Object](
    "bootstrap.servers"->ConfigUtil.getProperty("config.properties","bootstrap.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id"->ConfigUtil.getProperty("config.properties","group.id"),
    "auto.offset.reset"->"latest",
    "enable.auto.commit"->(true:java.lang.Boolean)
  )



  def getKafkaStream(ssc:StreamingContext,topic:String) ={
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, // 标配
      /*
      LocationStrategies
        preferConsistent:平均分配，每个分区的数据量平均
        PreferBroker:如果kafka和executor都在同一台设备，使用 PreferBroker，（不需要网络传输）
        PreferFixed：有数据倾斜时需要选这个
       */
      Subscribe[String, String](Set(topic), kafkaParams)
    ).map(_.value())
  }
}
