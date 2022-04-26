package com.thetradedesk.kongming.transform

import java.time.format.DateTimeFormatter
import com.thetradedesk.kongming._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyNegativeSampledBidRequestRecord}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, count, expr, hash, lit, pow, round, when,to_date, date_add,rand}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Dataset

object NegativeTransform {

  final case class NegativeSamplingBidRequestGrainsRecord(
                                                           BidRequestId: String,
                                                           LogEntryTime: java.sql.Timestamp,
                                                           UIID: String,
                                                           AdvertiserId: String,
                                                           CampaignId: String,
                                                           AdGroupId: String,
                                                           SupplyVendor: String,
                                                           Site: String,
                                                           RenderingContext: String,
                                                           Country: String,
                                                           DeviceType: String,
                                                           OperatingSystemFamily: String,
                                                           Browser: String
                                                         )

  final case class AggregateNegativesRecord (
                                              ConfigKey: String,
                                              ConfigValue: String,
                                              DataAggKey: String,
                                              DataAggValue: String,
                                              BidRequestId: String,
                                              UIID: String,
                                              LogEntryTime: java.sql.Timestamp,
                                            )

  def samplingWithConstantMod(
                               negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                               hashMode: Int =10
                             )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] = {
    negativeSamplingBidWithGrains.filter(hash($"BidRequestId")%hashMode<=0)
  }

  /**
   *  a variant of the method: "Real-Negative Subsampling"
   *  refer: https://atlassian.thetradedesk.com/confluence/display/EN/Training+Data+ETL?preview=/166441965/166445313/3340531.3412162.pdf
   * @param negativeSamplingBidWithGrains dataset to do sampling
   * @param grainsForSampling sample by those grains
   * @param normalizedFrequencyThreshold down sample grains whose frequency is higher than this threshold
   * @param frequencyToSampleRateCurveSmoother given an adgroup, smooth the curve between grain frequency and sample rate;  the large the tIndex, the less we sample large grains
   * @param minimumFrequencyPerGrain  if a grain's frequency is too small, for example, less than 5, discard it
   * @param totalBidPenaltyCurveSmoother given same grain's frequency, the more the adgroup's total bids is, the less sample rate is. large smoother will make sample rate less.
   * @return
   */
  def samplingByGrains(
                        negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                        grainsForSampling: Seq[String],
                        normalizedFrequencyThreshold: Double = 0.0001,
                        frequencyToSampleRateCurveSmoother: Double = 0.85,
                        minimumFrequencyPerGrain: Int = 5,
                        totalBidPenaltyCurveSmoother: Double = 0.0001
                      )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] ={
    val maxDecimal = 5

    val windowAggregationKeyGrain = Window.partitionBy(grainsForSampling.head, grainsForSampling.tail:_*)
    val windowAggregationKey = Window.partitionBy($"AdGroupId")

    negativeSamplingBidWithGrains
      .withColumn("BidFrequency", count($"BidRequestId").over(windowAggregationKeyGrain))
      .withColumn("TotalBid", count($"BidRequestId").over(windowAggregationKey))
      .withColumn("NormalizedFrequency", round($"BidFrequency"/$"TotalBid", maxDecimal))
      .withColumn("InverseNormalizedFrequency", lit(normalizedFrequencyThreshold)/$"NormalizedFrequency")
      .withColumn("SamplingRate",
        when(
          $"TotalBid"<=1/normalizedFrequencyThreshold, lit(1)  // if adgroup's total bids are too small, remain all the bids
        ).when(
          $"BidFrequency"< minimumFrequencyPerGrain, lit(0)  // if grain show up too rarely, discard
        ).when(
          $"InverseNormalizedFrequency"<1,   // if normalized frequency exceeds threshold, downsampling
          (
            pow($"InverseNormalizedFrequency",frequencyToSampleRateCurveSmoother)*pow(lit(1)/$"TotalBid", totalBidPenaltyCurveSmoother)
            )
        ).
          otherwise(lit(1))   // if normalized frequency doens't exceed threshold including normalizedfrequency is 0 due to resolution， remain all the bids
      )
      .withColumn("rand", rand())
      .filter($"rand"<$"SamplingRate")
      .selectAs[NegativeSamplingBidRequestGrainsRecord]

    /*
      InverseFrequency^tIndex is the sample rate of that grain, we need to translate it to mod.
      1- InverseFrequency^tIndex is the discard rate
     */
  }

  def aggregateNegatives(
                          dailyNegativeSampledBids: Dataset[DailyNegativeSampledBidRequestRecord],
                          adGroupPolicy: Dataset[AdGroupPolicyRecord])
                        (implicit prometheus:PrometheusClient): Dataset[AggregateNegativesRecord] = {
    /*
     join negatives with adgroup policy at different aggregation level
     The implementation is for future use cases when campaign/advertiser level aggregation come into play. For now the hard coded dataset has no such rows then the code will just passes two of of allNeagtives.
     */
    // todo: pre-check possible aggregation levels.
    broadcast(adGroupPolicy.filter($"DataAggKey"===lit("AdgroupId")).as("t1"))
      .join(dailyNegativeSampledBids.as("t2"), $"t1.DataAggValue"===$"t2.AdGroupId")
      .filter(date_add($"LogEntryTime", $"DataLookBack")>=date)
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
