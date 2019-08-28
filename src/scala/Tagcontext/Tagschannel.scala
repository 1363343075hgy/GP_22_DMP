package Tagcontext

import org.apache.spark.sql.Row
import utils.Tage

object Tagschannel extends Tage{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //获取渠道ID
   val Adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

     list:+=("CN"+Adplatformproviderid,1)

    (list)
  }


}
