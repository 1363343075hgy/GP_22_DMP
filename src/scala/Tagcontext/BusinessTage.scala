package Tagcontext

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import utils.{AmapUtil, JedisConnectionPool, Tage, Utils2Type}

object BusinessTage extends Tage{



  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    //获取经度纬度，过滤经纬度
    if (Utils2Type.toDouble(row.getAs[String]("long")) >= 73 &&
      Utils2Type.toDouble(row.getAs[String]("long")) <= 135 &&
      Utils2Type.toDouble(row.getAs[String]("lat")) >= 3 &&
      Utils2Type.toDouble(row.getAs[String]("lat")) <= 54) {
      //先去数据库获取商圈
      val business: String = getBusiness(long.toDouble,lat.toDouble)
        //判断缓存中是否有此商圈
        if (StringUtils.isNotBlank(business)){
          val lines: Array[String] = business.split(",")
          lines.foreach(f=>list:+=(f,1))
      }
    }
    list
  }

  /**
    * 获取商圈的信息
    * @param long
    * @param lat
    * @return
    */
      def getBusiness(long:Double,lat:Double):String ={
        //转换GeoHash字符串
        val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
        //去数据库查询
        val business = (redis_queryBusiness(geohash))
        //判断商圈是否为空
        if (business == null || business.length == 0){
          //通过经度纬度获取商圈
          val business: String = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
          //如果调用高德地图解析商圈，那么需要将此次商圈存入redis
          redis_insertBusiness(geohash,business)
        }
      business
    }

    /**
      * 获取商圈信息
      * @param geohash
      */
    def redis_queryBusiness(geohash: String) = {
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val business: String = jedis.get(geohash)
      jedis.close()
      business
    }

  /**
    * 存储商圈到redis
    */
    def redis_insertBusiness(geoHash:String,bussiness:String) ={
      val jedis: Jedis = JedisConnectionPool.getConnection()
      jedis.set(geoHash,bussiness)
      jedis.close()
    }





}
