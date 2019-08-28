package maintext


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import redis.clients.jedis.Jedis
import utils.JedisConnectionPool


object AppforrdiesContext {
  def main(args: Array[String]): Unit = {
    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取数据
    val file: RDD[String] = sc.textFile("E:\\FeiQ\\feiq\\Recv Files\\项目day01\\Spark用户画像分析\\app_dict.txt")

    file.map(_.split("\t",-1)).filter(_.length >= 5).foreachPartition(arr =>{

      val jedis: Jedis = JedisConnectionPool.getConnection()
      arr.foreach(arr=>{
        jedis.set(arr(4),arr(1))
      })
        jedis.close()
    })
    sc.stop()
  }
}
