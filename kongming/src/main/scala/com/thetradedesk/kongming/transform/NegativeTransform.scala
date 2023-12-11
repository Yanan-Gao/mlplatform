package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingRecord, AdGroupPolicyRecord, DailyNegativeSampledBidRequestRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import java.time.LocalDate

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
                                              BidRequestId: String,
                                              UIID: String,
                                              LogEntryTime: java.sql.Timestamp,
                                            )

  final case class CampaignInConfigGroupDataRecord (
                                           ConfigKey: String,
                                           ConfigValue: String,
                                           CampaignId: String
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
   * @param grainSamplingStartingFrequency when grain's frequency is larger than this, start to down sample grain
   * @param grainDiscardUntil  when grain's frequency is smaller than this, discard grain, because model can't learn from it
   * @param grainSampleRateSmoother the large the smoother, the more we sample large grains comparing to small grains
   * @return
   */
  def samplingByGrains(
                        negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                        grainsForSampling: Seq[String],
                        grainSamplingStartingFrequency: Int,
                        grainDiscardUntil: Int,
                        grainSampleRateSmoother: Double,
                        samplingSeed: Long
                      )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] ={

    val windowAggregationKeyGrain = Window.partitionBy(grainsForSampling.head, grainsForSampling.tail:_*)

    negativeSamplingBidWithGrains
      .withColumn("GrainFrequency", count($"BidRequestId").over(windowAggregationKeyGrain))
      .withColumn("FlatSampleRate", lit(grainSamplingStartingFrequency)/$"GrainFrequency")
      .withColumn("SamplingRate",
        when(
          $"GrainFrequency"< grainDiscardUntil, lit(0)  // if grain show up too rarely, discard all the bids
        ).when(
            $"GrainFrequency"< grainSamplingStartingFrequency, lit(1)
        ).otherwise(
          pow($"FlatSampleRate",  grainSampleRateSmoother)
        )
      )
      .withColumn("rand", rand(seed=samplingSeed))
      .filter($"rand"<$"SamplingRate")
      .selectAs[NegativeSamplingBidRequestGrainsRecord]

    /*
      InverseFrequency^tIndex is the sample rate of that grain, we need to translate it to mod.
      1- InverseFrequency^tIndex is the discard rate
     */
  }

  def aggregateNegatives(
                          date: LocalDate,
                          dailyNegativeSampledBids: Dataset[DailyNegativeSampledBidRequestRecord],
                          adGroupPolicy: Dataset[AdGroupPolicyRecord],
                          mapping: Dataset[AdGroupPolicyMappingRecord])
                        (implicit prometheus:PrometheusClient): Dataset[AggregateNegativesRecord] = {
    // Temporary hack. Do not duplicate a bid request by more than 50 times.
    val campaignConfigGroupMembership = multiLevelJoinWithPolicy[CampaignInConfigGroupDataRecord](mapping.select("AdGroupId", "CampaignId", "AdvertiserId"), adGroupPolicy, "inner")
      .groupBy("CampaignId").agg(collect_set("ConfigValue").as("ConfigValues"))

    dailyNegativeSampledBids.filter(date_add('LogEntryTime, lit(15)) >= date)
      .join(campaignConfigGroupMembership, Seq("CampaignId"), "inner")
      .withColumn("ConfigValues", when(size('ConfigValues) <= 20, 'ConfigValues).otherwise(slice(shuffle('ConfigValues), 1, 20)))
      .withColumn("ConfigKey", lit("AdGroupId"))
      .withColumn("ConfigValue", explode('ConfigValues))
      .selectAs[AggregateNegativesRecord]
  }
}
