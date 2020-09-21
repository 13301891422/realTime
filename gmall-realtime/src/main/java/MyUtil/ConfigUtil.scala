package MyUtil

import java.io.InputStream
import java.util.Properties

/**
 * @Description:
 * @author oranglzc
 * @creat 2020-07-15-8:50
 */
object ConfigUtil {
  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()
  properties.load(is)


  def getProperty(fileName:String,name:String)={
    properties.getProperty(name)
  }

  def main(args: Array[String]): Unit = {
    println(getProperty("config.properties","bootstrap.servers"))
  }

}
