package maintext



import Tagcontext.Tagsequitment
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.TagUtils

object Tageequitment {
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

    df.filter(TagUtils.OneUserId)
      .map(row=>{
        //取出用户
        val uid: String = TagUtils.getOneUserId(row)
        //接下来通过row数据 打上所有标签（按照需求）
        val adlist: List[(String, Int)] = Tagsequitment.makeTags(row)
        (uid,adlist.toMap)
      }).saveAsTextFile("E:\\FeiQ\\feiq\\Recv Files\\slaves5")
  }


}
