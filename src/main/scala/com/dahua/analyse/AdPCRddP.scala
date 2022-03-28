package com.dahua.analyse

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object AdPCRddP {
  def main(args: Array[String]): Unit = {

    // 判断参数是否正确。
//    if (args.length != 2) {
//      println(
//        """
//          |缺少参数
//          |inputpath
//          |""".stripMargin)
//      sys.exit()
//    }
    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("AdPCRddP").getOrCreate()

    var sc = spark.sparkContext


    val line: RDD[String] = sc.textFile("hdfs://192.168.137.11:8020/ood/2016-10-01_06_p1_invalid.1475274123982.log")
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val rdd:RDD[(String,Iterable[(String,Int)])] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    }).reduceByKey(_ + _).sortBy(_._2,false).map(x => (x._1._1,(x._1._2,x._2))).groupByKey(1)

    rdd.foreach(println(_))


    rdd.partitionBy(new MyPatition(30)).saveAsTextFile("hdfs://192.168.137.11:8020/output09")
    spark.stop()
    sc.stop()
  }

}

class MyPatition(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if (key == "江苏省") 0
    else if (key == "浙江省") 1
    else if (key == "山东省") 2
    else if (key == "广东省") 3
    else if (key == "安徽省") 4
    else if (key == "四川省") 5
    else if (key == "广西壮族自治区") 6
    else if (key == "河北省") 7
    else if (key == "江西省") 8
    else if (key == "陕西省") 9
    else if (key == "湖南省") 10
    else if (key == "湖北省") 11
    else if (key == "福建省") 12
    else if (key == "重庆市") 13
    else if (key == "北京市") 14
    else if (key == "河南省") 15
    else if (key == "辽宁省") 16
    else if (key == "内蒙古自治区") 17
    else if (key == "吉林省") 18
    else if (key == "黑龙江省") 19
    else if (key == "上海市") 20
    else if (key == "山西省") 21
    else if (key == "贵州省") 22
    else if (key == "云南省") 23
    else if (key == "新疆维吾尔自治区") 24
    else if (key == "宁夏回族自治区") 25
    else if (key == "甘肃省") 26
    else if (key == "青海省") 27
    else if (key == "海南省") 28
    else 29
  }
}