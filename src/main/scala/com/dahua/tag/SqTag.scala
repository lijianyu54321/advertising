package com.dahua.tag

import ch.hsr.geohash.GeoHash
import com.dahua.utils.RedisUtil
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object SqTag extends TagTrait {
  override def makeTargs(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val longitude: String = row.getAs[String]("longitude")// 经度
    val lat: String = row.getAs[String]("lat")// 维度
    // 不希望连接百度。
    //    val business: String = SNTools.getBusiness(lat+","+longitude)
    if(lat.toDouble > 3 && lat.toDouble < 54 && longitude.toDouble > 73 && longitude.toDouble < 136){
      val code: String = GeoHash.withCharacterPrecision(lat.toDouble,longitude.toDouble,8).toBase32
      val jedis: Jedis = RedisUtil.getJedis
      val business: String = jedis.get(code)
      map += "SN"+business -> 1
      jedis.close()
    }else{
    }
    map
  }
}
