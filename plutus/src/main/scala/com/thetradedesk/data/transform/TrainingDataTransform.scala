package com.thetradedesk.data.transform

import java.time.LocalDate

import com.thetradedesk.data._
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.Dataset

object TrainingDataTransform {

  val DEFAULT_SOURCE = "plutus"

  def loadCleanInputData(endDate: LocalDate, lookBack: Option[Int] = None, s3Path: String): Dataset[CleanInputData] = {
    getParquetData[CleanInputData](s3Path, endDate, Some(DEFAULT_SOURCE), lookBack)
  }

}
