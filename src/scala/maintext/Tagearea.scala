package maintext

import Tagcontext.Tagsarea
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.TagUtils

object Tagearea {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    df.filter(TagUtils.OneUserId).map(row=>{
      val Userid: String = TagUtils.getOneUserId(row)
      val adlist: List[(String, Int)] = Tagsarea.makeTags(row)
      (Userid,adlist.toMap)
    }).saveAsTextFile("E:\\FeiQ\\feiq\\Recv Files\\slaves6")
  }
}
