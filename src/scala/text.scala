import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object text {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

  }
}
