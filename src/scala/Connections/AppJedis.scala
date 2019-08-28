package Connections

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import utils.JedisConnectionPool

object AppJedis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext()
    //读取字段文件
    val dic = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\app_dict.txt")
    dic.map(_.split("\t",-1)).filter(_.length >=5).foreachPartition(arr=>{
      val jedis: Jedis = JedisConnectionPool.getConnection()
      arr.foreach(arr=>{
        jedis.set(arr(4),arr(1))
      })
        jedis.close()
    })
    sc.stop()
  }
}
