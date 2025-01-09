package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.date
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import java.time.format.DateTimeFormatter

object RSMV2SharedFunction {
  def getDateStr(pattern : String = "yyyyMMdd") = {
    val dateFormatter = DateTimeFormatter.ofPattern(pattern)
    date.format(dateFormatter)
  }

  def writeOrCache(writePathIfNeed : Option[String], overrideMode: Boolean, dataset: DataFrame, cache: Boolean = true): DataFrame = {
    if (writePathIfNeed.isEmpty) {
      return if(cache) dataset.cache() else dataset
    }
    val writePath = writePathIfNeed.get
    if (overrideMode || !FSUtils.fileExists(writePath + "/_SUCCESS")(spark)) {
      dataset.write.mode("overwrite").parquet(writePath)
    }
    spark.read.parquet(writePath)
  }


  val seedIdToSyntheticIdMapping =
    (mapping: Map[String, Int]) =>
      udf((origin: Array[String]) => {
        if (origin == null) Array.empty[Int]
        else origin.map(mapping.getOrElse(_, -1)).filter(_ >= 0)
      })

  object SubFolder extends Enumeration {
    type SubFolder = Value
    val Val, Holdout, Train = Value
  }

}
