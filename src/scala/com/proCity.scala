package com

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求指标二 统计地域各省市分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if(args.length != 2){
      println("目标参数不正确，退出程序")
      sys.exit()
    }

    //创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    //设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取本地文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("log")
    //指标统计
    val result: DataFrame = sQLContext.sql("select provincename,cityname,count(*) from log group by provincename,cityname")
    //加载配置文件 需要使用对应的依赖
    val load = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))

    result.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),properties)
    sc.stop()
  }
}
