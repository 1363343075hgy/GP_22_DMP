//package kafka
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object KafkaRedisOffset {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("offset").setMaster("local[*]")
//        //设置每秒中每个分区拉去kafka的速率
//      .set("spark.streaming.kafka.maxRatePerPartition","100")
//      //设置序列化机制
//      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
//    val scc = new StreamingContext(conf,Seconds(3))
//    //配置参数
//    //配置基本参数
//    //组名
//    val groupId = "zk002"
//    //topic
//    val topic = "hz1803b"
//    //指定Kafka的broker地址（SparkStreaming程序消费程序中，需要和Kafka的分区对应）
//    val brokerList = "192.168.126.11:9092"
//    //编写Kafka的配置参数
//    val kafkas = Map[String,Object](
//      "bootstrap.servers"->brokerList,
//      //kafka的Key和values解码方式
////      "key.deserializer"->classOf[StringDeserializer]
//
//
//
//    )
//
//
//
//
//
//
//
//
//
//  }
//}
