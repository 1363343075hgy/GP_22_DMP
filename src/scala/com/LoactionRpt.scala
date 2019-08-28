package com




import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 地域分布指标
  * Spark-SQL实现
  */
object LoactionRpt {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.2")
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //指定统计
    df.registerTempTable("logs")
    //发来的所有原始请求数
//    val df2: DataFrame =
//      sQLContext.sql("select sum(requestmode) requestmode,processnode from logs where requestmode = 1 and processnode >=1 group by requestmode,processnode")
//    df2.show()
//    //筛选满足有效条件的请求数量
//    val df3: DataFrame = sQLContext.sql("select sum(requestmode) requestmode,processnode from logs where requestmode = 1 and processnode >=2 group by requestmode,processnode")
//    df3.show()
//    //筛选满足广告请求条件的请求数量
//    val df4: DataFrame = sQLContext.sql("select sum(requestmode) requestmode,processnode from logs where requestmode = 1 and processnode =3 group by requestmode,processnode")
//    df4.show()
//    //参与竞价的次数
//    val df5: DataFrame = sQLContext.sql("select count(*) from logs where iseffective = 1 and isbilling = 1 and isbid = 1 group by provincename,cityname")
//    df5.show()
//    //成功竞价的次数
//    val df6: DataFrame = sQLContext.sql("select count(*) from logs where iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 group by provincename,cityname")
//    df6.show()
//    //针对广告主统计：广告在终端实际被展示的数量
//    val df7: DataFrame = sQLContext.sql("select count(*) from logs where requestmode = 2 and iseffective = 1 group by provincename,cityname")
//    df7.show()
//    //针对广告主统计：广告展示后被受众实际点击的数量
//    val df8: DataFrame = sQLContext.sql("select count(*) from logs where requestmode = 3 and iseffective = 1 group by provincename,cityname")
//    df8.show()
//    //相对于投放DSP广告的广告主来说满足广告成功展示每次消费WinPrice/1000
//    val df9: DataFrame = sQLContext.sql("select winprice/1000 from logs where iseffective = 1 and isbilling = 1 and iswin = 1 group by provincename,cityname,winprice")
//    df9.show()
//   //相对于投放DSP广告的广告主来说满足广告成功展示每次成本adpayment/1000
//    val df10: DataFrame = sQLContext.sql("select adpayment/1000 from logs where iseffective = 1 and isbilling = 1 and iswin = 1 group by provincename,cityname,adpayment")
//    df10.show()

    val df2: DataFrame = sQLContext.sql("select"+
     " sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) sumrequestmode1,"+
      "sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) sumrequestmode2,"+
      "sum(case when requestmode = 1 and processnode =3 then 1 else 0 end) sumrequestmode3,"+
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) sumwin1,"+
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end)sumwin2,"+
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) sumwin3,"+
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) sumwin4,"+
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end)sumwin5,"+
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end)sumwin6,"+
      "requestmode,"+
      "processnode,"+
"winprice/1000 as winprice,"+
"adpayment/1000 as adpayment "+
"from logs"+
" group by requestmode,"+
"processnode,"+
"winprice,"+ "adpayment")
df2.show()
//加载配置文件 需要使用对应的依赖
val load: Config = ConfigFactory.load()
val properties = new Properties()
properties.setProperty("user",load.getString("jdbc.user"))
properties.setProperty("password",load.getString("jdbc.password"))
df2.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),properties)
sc.stop()
}
}
