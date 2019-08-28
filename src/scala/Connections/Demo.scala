package Connections

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * List("1	小红 20","3 小明 33","5	小七	20","7	小王 60","9 小李	20","11	小美	30","13 小花")
  * 将上述数据创建RDD，然后根据数据结果图片进行处理，使用图计算技术，最后将结果保存值Mysql
  */


object Demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.makeRDD(List("1	小红 20", "3 小明 33", "5	小七	20", "7	小王 60", "9 小李	20", "11	小美	30", "13 小花"))
    val rdd2: RDD[Array[String]] = rdd1.map(_.split(","))


    val data: RDD[(Int, (String, Int))] = rdd2.map(x => {
      val id: Int = x(0).toInt
      val name: String = x(1).toString
      val age: Int = x(2).toInt
      (id, (name, age))
    })
    data.foreach(println)



  }
}
