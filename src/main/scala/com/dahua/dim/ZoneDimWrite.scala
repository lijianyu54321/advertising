package com.dahua.dim

import com.dahua.utils.RedisUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object ZoneDimWrite {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ZoneDimWrite")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    var Array(inputPath) = args
    sc.textFile(inputPath).map(line=>{
      val str: Array[String] = line.split("[:]", -1)
      (str(0),str(1))
    }).foreachPartition(ite=>{ //分区写入

      val jedis: Jedis = RedisUtil.getJedis

      ite.foreach(mapping=>{
        jedis.set(mapping._1,mapping._2)
      })

      jedis.close()

    })
  }
}
