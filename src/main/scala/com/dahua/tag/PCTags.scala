package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.mortbay.util.StringUtil

object PCTags extends TagTrait {
  override def makeTargs(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val provinceName: String = row.getAs[String]("provincename")
    val cityName: String = row.getAs[String]("cityname")
    if(StringUtils.isNotEmpty("provinceName")) map += "ZP"+provinceName->1
    if(StringUtils.isNotEmpty("cityname")) map += "ZP"+cityName->1
    map
  }
}
