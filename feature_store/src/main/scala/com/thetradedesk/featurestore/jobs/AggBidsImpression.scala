package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.{aggLevel, shouldTrackTDID}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import java.time.LocalDate


object AggBidsImpression extends FeatureStoreAggJob {

  // todo sven, this task have skew data issues, need to fix it
  override val sourcePartition: String = "bidsimpression"

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(lookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("WinRate", col("IsImp").cast("int"))
      .withColumnRenamed("UIID", "TDID")
      .filter(shouldTrackTDID(col(aggLevel)))
  }

}