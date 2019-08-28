package Tagcontext


import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import utils.Tage

object Appforredis extends Tage{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
  val row: Row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    if (!StringUtils.isNoneBlank(appname)){
      val appname: String = jedis.get(appid)
    }
      list:+=("App"+appname,1)
    list
  }

}
