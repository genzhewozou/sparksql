package Log.util

import com.ggstar.util.ip.IpHelper


object IpUtil {

  def getCity(ip:String): Unit ={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("192.168.29.17"))
  }
}
