package app
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import bean.StartupLog
import constant.Constant
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import MyUtil.MyKafkaUtil
import MyUtil.RedisUtil

/**
 * @Description:
 * @author oranglzc
 * @creat 2020-07-14-19:16
 */
object DauApp {

  //实现处理模块
  def main(args: Array[String]): Unit = {
    //TODO 1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //TODO 2. 获取流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)


    sourceStream.print()
    // 2.1 把每个json字符串的数据,父封装到一个样例类对象中
    val startupLogStream: DStream[StartupLog] = sourceStream.map(json => JSON.parseObject(json, classOf[StartupLog]))
    //TODO 3. 去重  过滤掉一件启动的那些设备的记录  从redis去读取已经启动过的设备的id



//    //TODO=============
//    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.transform(rdd => {
//      val client: Jedis = RedisUtil.getClient()
//      val mids: util.Set[String] = client.smembers(
//        Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
//      client.close()
//
//      val midsBD: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
//      //条件成立则保留，不成立则过滤
//      rdd
//        .filter(StartupLog => !midsBD.value.contains(StartupLog.mid))
//        .map(log => (log.mid, log))
//        .groupByKey()
//        .map {
//          case (_, logs) =>
//            logs.toList.sortBy(_.ts).head
//        }
//    })
//    ...
//    ...
//    ...

    //TODO 3. 开启流
    ssc.start()
    //TODO 4. 阻止main进程退出
    ssc.awaitTermination()
  }

}
