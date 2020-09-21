package MyUtil

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * @Description:
 * @author oranglzc
 * @creat 2020-07-15-11:31
 */
object RedisUtil {
  val conf =new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(30)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true)
  conf.setMaxWaitMillis(10000)
  conf.setTestOnCreate(true)
  conf.setTestOnBorrow(true)
  conf.setTestOnReturn(true)
  val pool=new JedisPool(conf,"hadoop105",6379)


  def getClient() ={
    pool.getResource
  }
  //测试
  def main(args: Array[String]): Unit = {
    println("尝试获取连接")
    val client=getClient()
    print("获取连接成功")
    client.set("age","10")
    println(client.get("age"))
    client.close()
  }
}

