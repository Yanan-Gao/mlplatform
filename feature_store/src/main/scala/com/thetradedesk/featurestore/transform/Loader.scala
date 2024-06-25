package com.thetradedesk.featurestore.transform
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object Loader {

  /** Get valid TrackingTag for CPA & Roas
   * @param date
   * @return trackingtagid, campaignid, reportingcolumnid
   */
  def loadValidTrackingTag(date: LocalDate): Dataset[ValidTrackingTagRecord] = {

    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, true)
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)

    val ccrcPreProcessed = ccrc
      .join(
        broadcast(campaignDS.select($"CampaignId", $"CustomCPATypeId", $"CustomROASTypeId", $"CustomCPAClickWeight", $"CustomCPAViewthroughWeight")),
        Seq("CampaignId"), "left"
      )

    val ccrcRoas = ccrcPreProcessed.filter(col("CustomROASTypeId") === 0 && $"ReportingColumnId" === 1)
      .union(
        ccrcPreProcessed.filter(col("CustomROASTypeId") > 0 && col("IncludeInCustomROAS"))
          .filter((col("CustomROASTypeId") === lit(1)) && ($"CustomROASWeight" =!= lit(0))
            or (col("CustomROASTypeId") === lit(2)) && ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight" =!= lit(0))
            or (col("CustomROASTypeId") === lit(3)) && ($"CustomROASWeight" * ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight") =!= lit(0)))
      ).selectAs[ValidTrackingTagRecord]
    val ccrcCpa = ccrcPreProcessed.filter(col("CustomCPATypeId") === 0 && $"ReportingColumnId" === 1).union(
      ccrcPreProcessed.filter(col("CustomCPATypeId") > 0 && col("IncludeInCustomCPA"))
        .filter((col("CustomCPATypeId") === lit(1)) && ($"Weight" =!= lit(0))
          or (col("CustomCPATypeId") === lit(2)) && ($"CustomCPAClickWeight" + $"CustomCPAViewthroughWeight" =!= lit(0)))
    ).selectAs[ValidTrackingTagRecord]

    ccrcCpa.union(ccrcRoas).distinct

  }

  case class ValidTrackingTagRecord (CampaignId: String,
                                     TrackingTagId: String,
                                     ReportingColumnId: Int)



}