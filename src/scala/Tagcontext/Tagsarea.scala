package Tagcontext

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tage

object Tagsarea extends Tage{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {


    var list: List[(String, Int)] = List[(String,Int)]()
    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //获取App名称和Appname

    val appname: String = row.getAs[String]("provincename")
    val appid: String = row.getAs[String]("cityname")
    if (StringUtils.isNoneBlank(appname))
      list:+=("ZP"+appname+","+"ZC"+appid,1)

    list
  }
}
