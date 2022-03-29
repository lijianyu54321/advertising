package com.dahua.tag

import ch.hsr.geohash.GeoHash
import com.dahua.toosl.SNTools
import com.dahua.utils.{RedisUtil, TagUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import redis.clients.jedis.Jedis

import java.util.UUID

object TagRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |缺少参数
          |inputpath,appMapping,stopwords  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("TagRpt").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath,app_Mapping,stopWords, outputPath) = args

    // 读取app_Mapping广播变量.
    val app_map: Map[String, String] = sc.textFile(app_Mapping).map(line => {
      val strs: Array[String] = line.split("[:]", -1)
      (strs(0), strs(1))
    }).collect().toMap

    // 广播app变量
    val broadCastAppMap: Broadcast[Map[String, String]] = sc.broadcast(app_map)

    // 停用词广播变量.
    val stopWordMap: Map[String, Int] = sc.textFile(stopWords).map((_, 0)).collect().toMap
    val broadcastStopWord: Broadcast[Map[String, Int]] = sc.broadcast(stopWordMap)


    var map = Map[String,String]()
    // 读取数据源,打数据标签.
    val df: DataFrame = spark.read.parquet(inputPath)
//    val logs = spark.read.parquet(inputPath)
//      .select("lat", "longitude")
//      .where(TagUtil.tagUserIdFilterParam)
//      .where("lat > 3 and lat < 54 and longitude > 73 and longitude < 136")
//      .distinct() // 写入redis.考虑。 开启连接，关闭连接。
//      .foreachPartition((itr: Iterator[Row]) => {
//        itr.foreach(row => {
//          val lat: String = row.getAs[String]("lat")
//          val longat: String = row.getAs[String]("longitude")
//          // 调用百度API,获得的商圈信息
//          val business: String = SNTools.getBusiness(lat + "," + longat)
//          if (business != null && StringUtils.isNotEmpty(business) && business != "") {
//            // GeoHash计算hash码                              39.9850750000,116.3161200000
//            val code: String = GeoHash.withCharacterPrecision(lat.toDouble, longat.toDouble, 8).toBase32
//
//            map  += code -> business
//
//          }
//        })
//      })
//    val  ll  = sc.broadcast(map)
    val TagDS: Dataset[(String, List[(String, Int)])] = df.where(TagUtil.tagUserIdFilterParam).map(row => {
      // 广告标签
      val adsMap: Map[String, Int] = AdsTags.makeTargs(row)
      // app标签.
      val appMap: Map[String, Int] = AppTags.makeTargs(row, broadCastAppMap.value)
      // 驱动标签
      val driverMap: Map[String, Int] = DriverTag.makeTargs(row)
      // 关键字标签
      val keyMap: Map[String, Int] = KeyTags.makeTargs(row, broadcastStopWord.value)
      // 地域标签
      val pcMap: Map[String, Int] = PCTags.makeTargs(row)
      // 商圈标签.
//      val stringToInt1: Map[String, Int] = Sq2Tag.makeTargs(row,ll.value)
//
          val stringToInt: Map[String, Int] = SqTag.makeTargs(row)


      // 获取用户ID
      //      (TagUtil.getUserId(row)(0),(adsMap++appMap++driverMap++keyMap++pcMap).toList)
      if (TagUtil.getUserId(row).size > 0) {
        (TagUtil.getUserId(row)(0), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ stringToInt).toList)
      } else {
        (UUID.randomUUID().toString.substring(0, 6), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ stringToInt).toList)
      }
    })
    TagDS.rdd.reduceByKey((list1,list2)=>{
      (list1++list2).toList
    }).saveAsTextFile(outputPath)

  }

}
