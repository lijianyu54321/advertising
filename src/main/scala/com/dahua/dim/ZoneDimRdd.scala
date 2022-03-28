package com.dahua.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimRdd {
    def main(args: Array[String]): Unit = {
      if (args.length != 1) {
        println(
          """
            |缺少参数
            |inputpath  outputpath
            |""".stripMargin)
        sys.exit()
      }


      // 创建sparksession对象
      var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val spark = SparkSession.builder().config(conf).appName("ZoneDimRdd").master("local[1]").getOrCreate()

      var sc = spark.sparkContext

      import spark.implicits._

      // 接收参数
      var Array(inputPath) = args

      val df: DataFrame = spark.read.parquet(inputPath)

      val dimRDD: Dataset[((String, String), List[Double])] = df.map(row => {
        // 获取字段。
        val requestMode: Int = row.getAs[Int]("requestmode")
        val processNode: Int = row.getAs[Int]("processnode")
        val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
        val iseffective: Int = row.getAs[Int]("iseffective")
        val isbilling: Int = row.getAs[Int]("isbilling")
        val isbid: Int = row.getAs[Int]("isbid")
        val iswin: Int = row.getAs[Int]("iswin")
        val adorderid: Int = row.getAs[Int]("adorderid")
        val winprice: Double = row.getAs[Double]("winprice")
        val adpayment: Double = row.getAs[Double]("adpayment")
        val province: String = row.getAs[String]("provincename")
        val cityname: String = row.getAs[String]("cityname")
        val appname: String = row.getAs[String]("appname")
        // 将维度写到方法里。

        val ysqqs: List[Double] = DimUtils.qqsRtp(requestMode, processNode)
        val cyjjs: List[Double] = DimUtils.jingjiaRtp(adplatformproviderid,iseffective, isbilling, isbid, iswin, adorderid)
        val ggzss: List[Double] = DimUtils.ggzjRtp(adplatformproviderid,requestMode, iseffective)
        val mjzss: List[Double] = DimUtils.mjjRtp(adplatformproviderid,requestMode, iseffective, isbilling)
        val ggxf: List[Double] = DimUtils.ggcbRtp(adplatformproviderid,iseffective, isbilling, iswin, winprice, adpayment)

        ((province, cityname), ysqqs ++ cyjjs ++ ggzss ++ mjzss ++ ggxf)
      })

      // 如何做聚合
      dimRDD.rdd.reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=>t._1+t._2)
      }).foreach(println)

    }


}
