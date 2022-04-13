package com.thetradedesk.kongming

import java.time.format.DateTimeFormatter

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyNegativeSampledBidRequestRecord, NegativeSamplingBidRequestGrainsRecord}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, count, expr, hash, lit, pow, round, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Dataset

object NegativeTransform {

  final case class AggregateNegativesRecord (
                                              DataAggKey: String,
                                              DataAggValue: String,
                                              BidRequestId: String,
                                              UIID: String
                                            )

  def samplingWithConstantMod(
                               negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                               hashMode: Int =10
                             )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] = {
    negativeSamplingBidWithGrains.filter(hash($"BidRequestId")%hashMode<=0)
  }

  /*
    Reference of the method samplingByGrains: "Real-Negative Subsampling"
    in https://atlassian.thetradedesk.com/confluence/display/EN/Training+Data+ETL?preview=/166441965/166445313/3340531.3412162.pdf
   */
  def samplingByGrains(
                        negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                        grainsForSampling: Seq[String],
                        normalizedFrequencyThreshold: Double = 0.0001,
                        tIndex: Double = 0.85,
                        minimumFrequencyPerGrain: Int = 5
                      )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] ={
    val maxDecimal = 5
    val minHashMod = 1
    val maxHashMod = 1e8.toInt

    val windowAggregationKeyGrain = Window.partitionBy(grainsForSampling.head, grainsForSampling.tail:_*)
    val windowAggregationKey = Window.partitionBy($"AdGroupId")

    negativeSamplingBidWithGrains
      .withColumn("BidFrequency", count($"BidRequestId").over(windowAggregationKeyGrain))
      .withColumn("TotalBid", count($"BidRequestId").over(windowAggregationKey))
      .withColumn("NormalizedFrequency", round($"BidFrequency"/$"TotalBid", maxDecimal))
      .withColumn("InverseNormalizedFrequency", lit(normalizedFrequencyThreshold)/$"NormalizedFrequency")
      .withColumn("HashModForDiscard",
        when(
          $"TotalBid"<=1/normalizedFrequencyThreshold, minHashMod  // if adgroup's total bids are too small, remain all the bids
        ).when(
          $"BidFrequency"< minimumFrequencyPerGrain, maxHashMod  // if grain show up too rarely, discard
        ).when(
          $"InverseNormalizedFrequency"<1,   // if normalized frequency exceeds threshold, downsampling
          (
            lit(0.5)/pow($"InverseNormalizedFrequency",tIndex)
            ).cast(IntegerType)
        ).
          otherwise(minHashMod)   // if normalized frequency doens't exceed threshold including normalizedfrequency is 0 due to resolution， remain all the bids
      )
      .filter(hash($"BidRequestId")%$"HashModForDiscard" ===lit(0))
      .selectAs[NegativeSamplingBidRequestGrainsRecord]

    /*
      Explaining lit(0.5)/pow($"InverseNormalizedFrequency",tIndex):
      Formula is mod = 1/(2*inverseFrequency^tIndex).
      InverseFrequency^tIndex is the sample rate of that grain, we need to translate it to mod.
      Since mod value has postive and negative, we need to times 2 to the sample rate.
      Then taking the reciprocal gives the mod.
     */
  }

  def aggregateNegatives(
                          dailyNegativeSampledBids: Dataset[DailyNegativeSampledBidRequestRecord],
                          adGroupPolicy: Dataset[AdGroupPolicyRecord])
                        (implicit prometheus:PrometheusClient): Dataset[AggregateNegativesRecord] = {
    /*
    Code for generate training data set, we don't have the job yet, I'll put it here for now:
        // maximum lookback from adgroup's policy
        val maxLookback = adGroupPolicy.agg(max("DataLookBack")).first.getInt(0)
        val dailyNegativeSampledBids = loadParquetData[DailyNegativeSampledBidRequestRecord](DailyNegativeSampledBidRequestDataSet.S3BasePath, date, lookBack = Some(maxLookback))
      .select("BidRequestId","UIID","AdGroupId","CampaignId","AdvertiserId", "date")

     */
    val formatterYMD = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    /*
     join negatives with adgroup policy at different aggregation level
     The implementation is for future use cases when campaign/advertiser level aggregation come into play. For now the hard coded dataset has no such rows then the code will just passes two of of allNeagtives.
     */
    // todo: pre-check possible aggregation levels.
    broadcast(adGroupPolicy.filter($"DataAggKey"===lit("AdGroupId")).as("t1"))
      .join(dailyNegativeSampledBids.as("t2"), $"t1.DataAggValue"===$"t2.AdGroupId")
      .filter(expr("date_add(date, DataLookBack)")>=date.format(formatterYMD))
      .select($"DataAggKey",$"DataAggValue",$"BidRequestId",$"UIID")
      //      .union(
      //        broadcast(adGroupPolicy.filter($"DataAggKey"===lit("CampaignId")).as("t1"))
      //          .join(allNegatives.as("t2"), $"t1.DataAggValue"===$"t2.CampaignId")
      //          .filter(expr("date_add(date, DataLookBack)")>=date.format(formatterYMD))
      //          .select($"DataAggKey",$"DataAggValue",$"BidRequestId",$"UIID")
      //      ).union(
      //      broadcast(adGroupPolicy.filter($"DataAggKey"===lit("AdvertiserId")).as("t1"))
      //        .join(allNegatives.as("t2"), $"t1.DataAggValue"===$"t2.AdvertiserId")
      //        .filter(expr("date_add(date, DataLookBack)")>=date.format(formatterYMD))
      //        .select($"DataAggKey",$"DataAggValue",$"BidRequestId",$"UIID")
      //    )
      .selectAs[AggregateNegativesRecord]
  }
}
