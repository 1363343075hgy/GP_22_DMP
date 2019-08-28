package com

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.RpUtils


/*
     媒体分析指标：
*/
object MediaAnalysis {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入路径有问题")
      sys.exit()
    }

    val Array(input,ouputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val map: Map[String, String] = sc.textFile("E://FeiQ//feiq//Recv Files//项目day01//Spark用户画像分析//app_dict.txt")
      .map(_.split("/t")).filter(_.length >= 5)
      .map(x => {
        (x(4), x(1))
      }).collect().toMap

     val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)
    //在Spark中处理结构化数据（行和列）的入口点。允许创建DataFrame对象以及执行SQL查询。
    val sQLContext = new SQLContext(sc)

    val df: DataFrame = sQLContext.read.parquet(input)
    //将数据进行处理，统计各个指标
    df.map(row =>{
      //把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值
      val appid = row.getAs[String]("appid")
      val appname = if (row.getAs[String]("appname").isEmpty){
        broadcast.value.getOrElse(appid,"null")
      }else{
        row.getAs[String]("appname")
      }
      ((appid,appname),RpUtils.request(requestmode,processnode)++RpUtils.click(requestmode,iseffective)++RpUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment))
    }).reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2)).saveAsTextFile(ouputPath)
    sc.stop()
  }
}
