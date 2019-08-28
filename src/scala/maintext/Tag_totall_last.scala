package maintext


import Tagcontext.{BusinessTage, TagKeyWord, TagsAd, TagsApp, Tagsarea, Tagschannel, Tagsequitment}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.TagUtils

object Tag_totall_last {

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
      //todo 调用Hbase API
      //加载配置文件
      val load: Config = ConfigFactory.load("application.conf")
      val hbaseTableName: String = load.getString("hbase.TableName")
      //创建Hadoop任务
      val configuration: Configuration = sc.hadoopConfiguration
      configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
      //创建HbaseConnection
      val hbconn: Connection = ConnectionFactory.createConnection(configuration)
      val hbadmin: Admin = hbconn.getAdmin
      //判断表是否可用
      if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
        //创建表操作
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
        val descriptor = new HColumnDescriptor("tags")
        tableDescriptor.addFamily(descriptor)
        hbadmin.createTable(tableDescriptor)
        hbadmin.close()
        hbconn.close()
      }

      //创建JobConf
      val jobconf = new JobConf(configuration)
      jobconf.setOutputFormat(classOf[TableOutputFormat])
      jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

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
      var list: List[(String, Int)] = List[(String,Int)]()
      //过滤符合Id的数据
      val baseRDD = df.filter(TagUtils.OneUserId)
        .map(row =>{
          val userList: List[String] = TagUtils.getAllUserId(row)
          (userList,row)
        })
      //构建点集合
      val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
        val row = tp._2
        //所有标签
        //        val UserId: String = TagUtils.getOneUserId(row)
        val adTags: List[(String, Int)] = TagsAd.makeTags(row)
        val appList: List[(String, Int)] = TagsApp.makeTags(row, broadcast)
        val channelTags: List[(String, Int)] = Tagschannel.makeTags(row)
        val equitmentTags: List[(String, Int)] = Tagsequitment.makeTags(row)
        val keywordList = TagKeyWord.makeTags(row, broadcast)
        val areaTags: List[(String, Int)] = Tagsarea.makeTags(row)
        val businessTags: List[(String, Int)] = BusinessTage.makeTags(row)
        val AllTag = adTags ++ appList ++ channelTags ++ equitmentTags ++ keywordList ++ areaTags ++ businessTags
        //List((String,Int))
        //保证其中一个点携带着所有标签，同时也保留所有userId
        val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ AllTag
        //处理所有点的集合
        tp._1.map(uId => {
          if (tp._1.head.equals(uId)) {
            (uId.hashCode.toLong, VD)
          } else {
            (uId.hashCode.toLong, List.empty)
          }
        })
      })
      val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
        tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
      })

//      vertiesRDD.take(50).foreach(println)

//    构建图
      val graph = Graph(vertiesRDD,edges)
      val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
      vertices.join(vertiesRDD).map{
        case (uId,(conId,tagsAll))=>(conId,tagsAll)
      }.reduceByKey((list1,list2)=>{
        //聚合所有标签
        (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
      }).take(20).foreach(println)

      sc.stop()
    }

}
