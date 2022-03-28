package com.dahua.analyse

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object AdPCSortRdd {
  def main(args: Array[String]): Unit = {

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("AdPCSortRdd").getOrCreate()

    var sc = spark.sparkContext

    var Array(inputPath,outputPath) = args
    val line: RDD[String] = sc.textFile("hdfs://192.168.137.11:8020/ood/2016-10-01_06_p1_invalid.1475274123982.log")
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val rdd = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    }).reduceByKey(_ + _)
    rdd.cache()
    val num: Int = rdd.map(_._1._1).distinct().count().toInt


    rdd.partitionBy(new MyPartition(num))saveAsTextFile("hdfs://192.168.137.11:8020/output07")
    spark.stop()
    sc.stop()
  }
}
class  MyPartition (val count:Int) extends  Partitioner{
  override def numPartitions: Int = count

  private var num = -1

  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()

  override def getPartition(key: Any): Int = {
    val value :String =key.toString
    val str: String = value.substring(1, value.indexOf(","))
    println(str)

    if(map.contains(str)){
      map.getOrElse(str,num)
    }else{
      num +=1
      map.put(str,num)
      num
    }
  }
}


