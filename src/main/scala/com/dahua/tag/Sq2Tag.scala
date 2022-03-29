package com.dahua.tag

import ch.hsr.geohash.GeoHash
import com.dahua.utils.RedisUtil
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Sq2Tag extends TagTrait {
  override def makeTargs(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val longitude: String = row.getAs[String]("longitude")// 经度
    val lat: String = row.getAs[String]("lat")// 维度
    val logs: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
    if(lat.toDouble > 3 && lat.toDouble < 54 && longitude.toDouble > 73 && longitude.toDouble < 136){
      val code: String = GeoHash.withCharacterPrecision(lat.toDouble,longitude.toDouble,8).toBase32
      logs.map(x=>(x._1,x._2)).foreach(itr => {
        if (itr._1.contains(code)){
          map += "SN"+itr._2 -> 1
        }
      })
    }else{
      map += "SN" -> 0
    }
    map
  }
}
