package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row



object AdsTags extends TagTrait {

  /**
   * 标签格式:告位类型名称
   */
  override def makeTargs(args: Any*): Map[String, Int] = {
    var map =  Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val adspacetype = row.getAs[Int]("adspacetype")
    val adspacetypename = row.getAs[String]("adspacetypename")
    if (adspacetype > 9){
      map += "LC" + adspacetype -> 1
    } else{
      map += "LC0" + adspacetype -> 1
    }
    if (StringUtils.isNotEmpty(adspacetypename)){
      map += "LN" + adspacetypename -> 1
    }
    map
  }
}
