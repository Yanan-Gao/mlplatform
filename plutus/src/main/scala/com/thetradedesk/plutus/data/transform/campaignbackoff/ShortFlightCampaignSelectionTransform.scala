package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignFlightDataset, CampaignFlightRecord, ShortFlightCampaignsDataset, ShortFlightCampaignsSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{MaxTestBucketExcluded, MinTestBucketIncluded, bucketCount, getTestBucketUDF}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.campaignbackoff.CampaignAdjustmentsJob.shortFlightCampaignCounts
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit, unix_timestamp}

import java.sql.Timestamp
import java.time.{LocalDate, ZoneOffset}

object ShortFlightCampaignSelectionTransform {

  val MinimumShortFlightFloorBuffer = 0.35

  def getEligibleShortFlightCampaigns(date: LocalDate,
                                      flightData: Dataset[CampaignFlightRecord]
                                     ):Dataset[ShortFlightCampaignsSchema] = {
    // Automatically handle all short flight campaigns (not checking if their PC enabled or DA)
    // If campaign starting later today after backoff job runs, we won't know whether they are DA or not
    val minStartTimeForShortFlights = Timestamp.from(date.minusDays(2).atStartOfDay().atZone(ZoneOffset.UTC).toInstant)
    val maxEndTimeForShortFlights = Timestamp.from(date.plusDays(4).atTime(5, 0).atZone(ZoneOffset.UTC).toInstant)

    val postBackoffShortFlights_maxStartTime = Timestamp.from(date.plusDays(2).atTime(5, 0).atZone(ZoneOffset.UTC).toInstant)

    flightData
      .withColumn("FlightDurationHours",
        (unix_timestamp($"EndDateExclusiveUTC") - unix_timestamp($"StartDateInclusiveUTC")) / 3600.0
      )
      .filter(
        $"StartDateInclusiveUTC" >= minStartTimeForShortFlights &&
          $"EndDateExclusiveUTC" <= maxEndTimeForShortFlights &&
          $"flightDurationHours" <= 48 &&
          (
            $"IsCurrent" === 1 || // Active campaigns that started up to 2 days ago and is still eligible for short-flight backoff
              ($"StartDateInclusiveUTC" >= date.plusDays(1) && $"StartDateInclusiveUTC" < postBackoffShortFlights_maxStartTime) // Campaign starting later today after backoff job runs
            )
      )
      // Cannot filter for DA-only before campaign starts so may include non-DA, but backoff ultimately won't get applied to non-DA.
      // Filter for Buffer Test Campaigns only
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
      // Set MinimumShortFlightFloorBuffer
      .withColumn("MinimumShortFlightFloorBuffer", lit(MinimumShortFlightFloorBuffer))
      .select("CampaignId", "MinimumShortFlightFloorBuffer").distinct()
      .as[ShortFlightCampaignsSchema]
  }

  def transform(date: LocalDate,
                fileCount: Int
               ): Dataset[ShortFlightCampaignsSchema] = {

    val campaignFlights = CampaignFlightDataset.readLatestDataUpToIncluding(date.plusDays(1))

    val eligibleShortFlightCampaigns = getEligibleShortFlightCampaigns(date, campaignFlights)

    // Write the short flight campaign data and rollback dataset to S3
    ShortFlightCampaignsDataset.writeData(date = date, dataset = eligibleShortFlightCampaigns, filecount = fileCount)

    val shortFlightCampaignsCount = eligibleShortFlightCampaigns.count()
    shortFlightCampaignCounts.labels(Map("status" -> "ShortFlightCampaigns")).set(shortFlightCampaignsCount)

    eligibleShortFlightCampaigns
  }
}
