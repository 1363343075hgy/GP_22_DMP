package Tagcontext


import org.apache.spark.sql.Row
import utils.Tage

object TypeTag extends Tage {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]

    val Type: String = row.getAs[String]("Type")
    //获取渠道ID
    list:+=("Type"+Type,1)

    (list)


  }
}
