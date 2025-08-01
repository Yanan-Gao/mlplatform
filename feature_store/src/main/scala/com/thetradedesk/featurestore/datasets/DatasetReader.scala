package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.utils.StringUtils
import com.thetradedesk.featurestore.utils.StringUtils.getDateStr
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.DataFrame

import java.time.LocalDate

object DatasetReader {

  // Trying to find aggregatedSeeds from last n days, starting from context.dateStr
  def readLatestDataset(path: String, startDate: LocalDate, loopback: Int = 7): DataFrame = {
    for (i <- 0 until loopback) {

      val sourcePath = StringUtils.applyNamedFormat(path, Map("dateStr" -> getDateStr(startDate.minusDays(i))))
      val sourceSuccessFilePath = s"${sourcePath}/_SUCCESS"

      if (FSUtils.fileExists(sourceSuccessFilePath)(spark)) {
        return spark.read.parquet(sourcePath)
      }
      println(s"Unable to find _SUCCESS file in path ${sourceSuccessFilePath}")
    }
    throw new RuntimeException("Dataset not existing")
  }

}
