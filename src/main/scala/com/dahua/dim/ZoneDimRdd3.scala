package com.dahua.dim

import com.dahua.bean.LogBean
import com.dahua.utils.RedisUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object ZoneDimRdd3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  appmapping outputpath
          |""".stripMargin)
      sys.exit()
    }
    var Array(inputPath,out) = args
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("ZoneDimRdd3")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val out1 = sc.textFile(inputPath).map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      !t.appid.isEmpty
    })


    val out2 = out1.mapPartitions(log => {

      val jedis: Jedis = RedisUtil.getJedis

      var res = List[LogBean]()
      while (log.hasNext) {
        var bean: LogBean = log.next()
        if (bean.appname == "" || bean.appname.isEmpty) {
          var str = jedis.get(bean.appid)
          bean.appname = str
        }
        res.::=(bean)
      }
      jedis.close()
      res.iterator
    })

    out2.saveAsTextFile(out)
  }

}
