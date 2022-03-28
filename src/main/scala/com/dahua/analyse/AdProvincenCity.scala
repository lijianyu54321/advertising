package com.dahua.analyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AdProvincenCity {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("AdProvincenCity").master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    // 接收参数
    var Array(inputPath, outputPath) = args
    val df = spark.read.parquet(inputPath)
    df.createTempView("log")
    val df1 = spark.sql("select provincename,cityname,count(*) as pccount from log group by provincename,cityname Order By provincename,pccount DESC")
    // 判断目标目录下是否有文件？
    val config = sc.hadoopConfiguration
    // 文件系统对象。
    val fs = FileSystem.get(config)
    // 判断目标目录下是否有文件？
    val path = new Path(outputPath)
    if(fs.exists(path)){ fs.delete(path,true)}

    df1.coalesce(1).write.json(outputPath)

    spark.stop()
    sc.stop()


  }

}
