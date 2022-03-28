package com.dahua.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdPCSort {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin)
      sys.exit()
    }

    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("AdPCSort").master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    // 接收参数
    var Array(inputPath) = args
    val df = spark.read.parquet(inputPath)
    df.createTempView("log")
    val df1 = spark.sql("select provincename,cityname,count(*) as pccount from log group by provincename,cityname Order By provincename,pccount DESC")
    df1.show()

    spark.stop()
    sc.stop()
  }
}
