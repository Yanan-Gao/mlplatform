package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.transform.Merger.joinDataFrames
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import java.time.LocalDate


object AggBidsImpression extends FeatureStoreAggJob {
  override def jobName: String = "bidsimpression"
  override def jobConfig = new FeatureStoreAggJobConfig( s"${getClass.getSimpleName.stripSuffix("$")}.json" )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val inputDf = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(lookBack), source = Some(GERONIMO_DATA_SOURCE))
    inputDf.withColumn("WinRate", col("IsImp").cast("int"))
  }

}