package com.dahua.tag

import com.dahua.utils.TagUtil
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DmpTag_02 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |com.dahua.tag1.DmpTag_01
          |缺少参数
          |inputPath
          |appmapping
          |stopword
          |outputPath
        """.stripMargin)
      sys.exit()
    }

    val Array(inputPath, appmapping,stopword) = args
    // Sparkconf对象。
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // SparkSession对象。
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val load: Config = ConfigFactory.load()
    val hbaseTableName: String = load.getString("hbase.table.name")
    val configuration: Configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)

    val hbAdmin: Admin = hbConn.getAdmin
    if(!hbAdmin.tableExists(TableName.valueOf(hbaseTableName))){
      println("表不存在，创建中****")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val columnFamily = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(columnFamily)
      hbAdmin.createTable(tableDescriptor)
      hbAdmin.close()
      hbConn.close()
    }

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)





    // 读取 app名称。
    val appMap: RDD[String] = sc.textFile(appmapping)
    val appMap1: Map[String, String] = appMap.map(line => {
      val words: Array[String] = line.split("[:]", -1)
      (words(0), words(1))
    }).collect().toMap

    // 读取停用词。
    val stopWrodRDD: RDD[String] = sc.textFile(stopword)
    val stopWord2Map: Map[String, Int] = stopWrodRDD.map(line => {
      val field: Array[String] = line.split(",")
      (field(0), 1)
    }).collect().toMap

    // 使用广播变量。将appMapping进行广播。
    val broadCastAppMap: Broadcast[Map[String, String]] = sc.broadcast(appMap1)
    // 将停用词使用广播变量进行广播。
    val broadcastStopWord: Broadcast[Map[String, Int]] = sc.broadcast(stopWord2Map)

    // 最终的效果。
    // imei：3472934723472934，LC01->1,LNbanner->1,APP爱奇艺->1`CNxxx->1,D00010001->1,D00020001>1
    // key ->Id：   value-> List(map)

    // key如何获得。
    val df: DataFrame = spark.read.parquet(inputPath)
    val tagFilter: Dataset[Row] = df.where(TagUtil.tagUserIdFilterParam)
    val tag: Dataset[(String, List[(String, Int)])] = tagFilter.map(row => {
      // 广告标签
      val adsMap: Map[String, Int] = AdsTags.makeTargs(row)
      // app标签.
      val appMap: Map[String, Int] = AppTags.makeTargs(row, broadCastAppMap.value)
      // 驱动标签
      val driverMap: Map[String, Int] = DriverTag.makeTargs(row)
      // 关键字标签
      val keyMap: Map[String, Int] = KeyTags.makeTargs(row, broadcastStopWord.value)

      (TagUtil.getUserId(row)(0), (adsMap ++ appMap ++ driverMap ++ keyMap).toList)

    })

    tag.rdd.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).map{
      case(userId,userTags)=>{
        val put = new Put(Bytes.toBytes(userId))// 使用userId，当做rowkey
        val tags: String = userTags.map(t=>t._1+","+t._2).mkString(",")
        var day = "2020-09-30"
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"day${day}"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)

  }

}
