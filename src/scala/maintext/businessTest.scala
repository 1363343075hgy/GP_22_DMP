package maintext

import Tagcontext.BusinessTage
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object businessTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val file: DataFrame = ssc.read.parquet("E:\\FeiQ\\feiq\\saves3")
    file.map(row =>{
      val business: List[(String, Int)] = BusinessTage.makeTags(row)
      business
    }).foreach(println)

  }
}
