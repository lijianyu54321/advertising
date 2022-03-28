package com.dahua.toosl

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Internet01 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Internet01").master("local[*]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._
    var Array(inputPath, outputPath) = args
    val arr: RDD[String] = sc.textFile(inputPath)
    val sql1 = arr.map(_.split("[,]")).filter(x => x.length>=85).map(x =>  new Internet(
      x(0),
      x(1).toInt,
      x(2).toInt,
      x(3).toInt,
      x(4).toInt,
      x(5),
      x(6),
      x(7).toInt,
      x(8).toInt,
      x(9).toDouble,
      x(10).toDouble,
      x(11),
      x(12),
      x(13),
      x(14),
      x(15),
      x(16),
      x(17).toInt,
      x(18),
      x(19),
      x(20),
      x(21),
      x(22),
      x(23),
      x(24),
      x(25),
      x(26).toInt,
      x(27),
      x(28).toInt,
      x(29),
      x(30).toInt,
      x(31).toInt,
      x(32).toInt,
      x(33),
      x(34).toInt,
      x(35).toInt,
      x(36).toInt,
      x(37),
      x(38).toInt,
      x(39).toInt,
      x(40).toDouble,
      x(41).toDouble,
      x(42).toInt,
      x(43),
      x(44).toInt,
      x(45).toInt,
      x(46),
      x(47),
      x(48),
      x(49),
      x(50),
      x(51),
      x(52),
      x(53),
      x(54),
      x(55),
      x(56),
      x(57).toInt,
      x(58).toDouble,
      x(59).toInt,
      x(60).toInt,
      x(61),
      x(62),
      x(63),
      x(64),
      x(65),
      x(66),
      x(67),
      x(68),
      x(69),
      x(70),
      x(71),
      x(72),
      x(73).toInt,
      x(74).toDouble,
      x(75).toDouble,
      x(76).toDouble,
      x(77).toDouble,
      x(78).toDouble,
      x(79),
      x(80),
      x(81),
      x(82),
      x(83),
      x(84).toInt
    ))


    val  df:DataFrame = sql1.toDF()
    df.write.parquet(outputPath)

//    df.createTempView("internet")

//    session.sql("select provincename,cityname ,count(*) as o from internet group by provincename,cityname ").show()

    df.show()
    sc.stop()
    spark.stop()




    //    val inter1 = df.as[Internet]
    //0bb49045000057eee4ed3a580019ca06,0,0,0,100002,未知,26C07B8C83DB4B6197CEB80D53B3F5DA,1,1,0,0,2016-10-01 06:19:17
    // ,139.227.161.115,com.apptreehot.horse,马上赚钱,AQ+KIQeBhehxf6x988FFnl+CV00p,A10%E5%8F%8C%E6%A0%B8,1,4.1.1,,768,
    // 980,116.3161200000,39.9850750000,上海市,上海市,4,未知,3,Wifi,0,0,2,插屏,1,2,6,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,555,
    // 240,290,,,,,,,,,,,AQ+KIQeBhehxf6x988FFnl+CV00p,,1,0,0,0,0,0,,,mm_26632353_8068780_27326559,2016-10-01 06:19:17,,
  }
}


case class Internet(
                     sessionid: String,
                     advertisersid: Int,
                     adorderid: Int,
                     adcreativeid: Int,
                     adplatformproviderid: Int,
                     sdkversion: String,
                     adplatformkey: String,
                     putinmodeltype: Int,
                     requestmode: Int,
                     adprice: Double,
                     adppprice: Double,
                     requestdate: String,
                     ip: String,
                     appid: String,
                     appname: String,
                     uuid: String,
                     device: String,
                     client: Int,
                     osversion: String,
                     density: String,
                     pw: String,
                     ph: String,
                     long1: String,
                     lat: String,
                     provincename: String,
                     cityname: String,
                     ispid: Int,
                     ispname: String,
                     networkmannerid: Int,
                     networkmannername:String,
                     iseffective: Int,
                     isbilling: Int,
                     adspacetype: Int,
                     adspacetypename: String,
                     devicetype: Int,
                     processnode: Int,
                     apptype: Int,
                     district: String,
                     paymode: Int,
                     isbid: Int,
                     bidprice: Double,
                     winprice: Double,
                     iswin: Int,
                     cur: String,
                     rate: Double,
                     cnywinprice: Double,
                     imei: String,
                     mac: String,
                     idfa: String,
                     openudid: String,
                     androidid: String,
                     rtbprovince: String,
                     rtbcity: String,
                     rtbdistrict: String,
                     rtbstreet: String,
                     storeurl: String,
                     realip: String,
                     isqualityapp: Int,
                     bidfloor: Double,
                     aw: Int,
                     ah: Int,
                     imeimd5: String,
                     macmd5: String,
                     idfamd5: String,
                     openudidmd5: String,
                     androididmd5: String,
                     imeisha1: String,
                     macsha1: String,
                     idfasha1: String,
                     openudidsha1: String,
                     androididsha1: String,
                     uuidunknow: String,
                     userid: String,
                     iptype: Int,
                     initbidprice: Double,
                     adpayment: Double,
                     agentrate: Double,
                     lomarkrate: Double,
                     adxrate: Double,
                     title: String,
                     keywords: String,
                     tagid: String,
                     callbackdate: String,
                     channelid: String,
                     mediatype: Int
                   )

