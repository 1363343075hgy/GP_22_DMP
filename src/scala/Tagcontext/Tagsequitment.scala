package Tagcontext

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tage

object Tagsequitment extends Tage{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //获取client: Int
    val client: Int = row.getAs[Int]("client")
    if (client == 1){
      list:+=("Android D00010001",1)
    }else if(client ==2 ){
      list:+=("IOS D00010002",1)
    }else if (client == 3){
      list:+=("WinPhone D00010003",1)
    }else {
      list:+=("其 他 D00010004",1)
    }
    //networkmannername: String,设备联网方式名称
    val networkmannername: String = row.getAs[String]("networkmannername")
    val networkmannerid: Int = row.getAs[Int]("networkmannerid")
    /**
      * networkmannerid: Int,	联网方式 id
      * networkmannername:
      * String,	联网方式名称
      */
      if(StringUtils.isNoneBlank(networkmannername))
        list :+= (networkmannername + " D0002000" + networkmannerid, 1)
    //获取运营商
    // ispid: Int,	运营商 id
    //ispname: String,	运营商名称
    val ispid: Int = row.getAs[Int]("ispid")
    val ispname: String = row.getAs[String]("ispname")
    if(StringUtils.isNoneBlank(networkmannername))
      list :+= (ispname + " D0003000" + ispid, 1)
    list
  }
}
