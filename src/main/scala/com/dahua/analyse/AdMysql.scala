package com.dahua.analyse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object AdMysql {
  def main(args: Array[String]): Unit = {

    // 判断参数。
    if (args.length != 1) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          """.stripMargin)
      sys.exit()
    }
    val Array(inputPath) = args
    val spark = SparkSession.builder().appName("AdMysql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")
    val sql = "select provincename,cityname,count(*) as pcsum from log  group by provincename,cityname"
    val procityCount: DataFrame = spark.sql(sql)
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))
    procityCount.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)
    spark.stop()
  }

}
