package Tagcontext

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import utils.Tage

object TagsApp extends Tage{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
    //解析参数

    val row: Row = args(0).asInstanceOf[Row]
    val appbroadcast: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //获取App名称和Appname
//      val jedis: Jedis = args(1).asInstanceOf[Jedis]
    /**
      * StringUtils中一共有130多个方法，并且都是static的，所以我们可以这样调用StringUtils.xxx()
      *
      * public static boolean isEmpty(String str)判断某字符串是否为空，为空的标准是str==null或str.length()==0
      *
      * public static boolean isNotEmpty(String str)判断某字符串是否非空，等于!isEmpty(String str)
      */
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    //空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("App"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("App"+appbroadcast.value.getOrElse(appid,appid),1)
    }

//    if(StringUtils.isNotBlank(appname)){
//        jedis.get(appid)
//    }
//    list:+=("App"+appname,1)

    list
  }
}
