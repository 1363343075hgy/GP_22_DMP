package maintext
import Tagcontext.TagKeyWord
import utils.TagUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TagKeyWordContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 读取字段文件
    val map = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row=>{
      // 取出用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）
      val keywordList = TagKeyWord.makeTags(row,bcstopword)
      (userId,keywordList)
    }).saveAsTextFile("E:\\FeiQ\\feiq\\Recv Files\\slaves7")

  }
}
