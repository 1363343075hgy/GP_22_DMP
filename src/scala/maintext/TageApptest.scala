package maintext

import Tagcontext.TagsApp
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.TagUtils

object TageApptest {
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
    //读取字段文件
    val map: Map[String, String] = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collect().toMap

    //将处理好的数据使用数据广播
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)
    //获取停用词库
    val stopword: collection.Map[String, Int] = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_,0)).collectAsMap()
    val stopbroadcast: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopword)
    //过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
    //接下来所有的标签都在内部实现
      .map(row=>{
      //取用户id
      val userId: String = TagUtils.getOneUserId(row)
      val appList: List[(String, Int)] = TagsApp.makeTags(row,broadcast)



      (userId,appList.toMap)
    }).saveAsTextFile("E:\\FeiQ\\feiq\\Recv Files\\slaves3")

  }
}
