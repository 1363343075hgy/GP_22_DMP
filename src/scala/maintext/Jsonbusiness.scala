package maintext


import Tagcontext.TypeTag
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}



object Jsonbusiness {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)
    val strings: Array[String] = sc.textFile("E://json.txt").collect()

    strings.map(string=>{

      val typeList: List[(String, Int)] = TypeTag.makeTags(string)
      val jsonparse = JSON.parseObject(string)
      val status: Int = jsonparse.getIntValue("status")
      if(status==0) return ""
      val regeocode=jsonparse.getJSONObject("regeocode")

      if(regeocode==null || regeocode.keySet().isEmpty) return ""

      val pois= regeocode.getJSONArray("pois")

      if(pois==null || pois.isEmpty) return ""
      var list1=List[(String,Int)]()
      var list2=List[(String,Int)]()
      for(item <- pois.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          if(json.getString("businessarea")!="" || !json.getString("businessarea").isEmpty){

            list1:+=(json.getString("businessarea"),1)
          }
          //          println(json.getString("businessarea"))
        }
      }

      for(item <- pois.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val arrType: Array[String] = json.getString("type").split(";")
          arrType.foreach(x=>{
            list2:+=(x,1)
          })
        }
      }
      println(list1.groupBy(_._1).mapValues(_.size).toBuffer)
      println(list2.groupBy(_._1).mapValues(_.size).toBuffer)
      println(typeList.groupBy(_._1).mapValues(_.size).toBuffer)
    })
  }
}
      //判断状态是否成功
