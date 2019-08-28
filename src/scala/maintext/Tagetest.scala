package maintext

import Tagcontext.TagsAd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.TagUtils

object Tagetest {
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
    // 过滤符合Id的数据
    val file: RDD[(String, Map[String, Int])] = df.filter(TagUtils.OneUserId)
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据 打上所有标签（按照需求）
      val adList: List[(String, Int)] = TagsAd.makeTags(row)
      (userId, adList.toMap)
    })

    file.saveAsTextFile("E:\\FeiQ\\feiq\\Recv Files\\slaves2")
  }
}
