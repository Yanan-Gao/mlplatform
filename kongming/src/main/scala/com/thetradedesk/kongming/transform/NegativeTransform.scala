package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyNegativeSampledBidRequestRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, date_add, hash, lit, pow, rand, when}
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
   * @param grainSamplingStartingFrequency when grain's frequency is larger than this, start to down sample grain
   * @param grainDiscardUntil  when grain's frequency is smaller than this, discard grain, because model can't learn from it
   * @param grainSampleRateSmoother the large the smoother, the more we sample small grains comparing to large grains
   * @param totalBidPenalty  the more the adgroup's total bids is, the less sample rate is. large smoother will make sample rate less.
   * @return
   */
  def samplingByGrains(
                        negativeSamplingBidWithGrains: Dataset[NegativeSamplingBidRequestGrainsRecord],
                        grainsForSampling: Seq[String],
                        grainSamplingStartingFrequency: Int,
                        grainDiscardUntil: Int,
                        grainSampleRateSmoother: Double,
                        totalBidPenalty: Double
                      )(implicit prometheus:PrometheusClient): Dataset[NegativeSamplingBidRequestGrainsRecord] ={

    val windowAggregationKeyGrain = Window.partitionBy(grainsForSampling.head, grainsForSampling.tail:_*)
    val windowAggregationKey = Window.partitionBy($"AdGroupId")

    negativeSamplingBidWithGrains
      .withColumn("GrainFrequency", count($"BidRequestId").over(windowAggregationKeyGrain))
      .withColumn("TotalBid", count($"BidRequestId").over(windowAggregationKey))
      .withColumn("FlatSampleRate", lit(grainSamplingStartingFrequency)/$"GrainFrequency")
      .withColumn("SamplingRate",
        when(
          $"GrainFrequency"< grainDiscardUntil, lit(0)  // if grain show up too rarely, discard all the bids
        ).when(
          $"FlatSampleRate"<1,   // if grain frequency exceeds threshold, down sample grain
          (
            pow($"FlatSampleRate",grainSampleRateSmoother)*pow(lit(1)/$"TotalBid",  totalBidPenalty)
            )
        ).
          otherwise(lit(1))   // if grain frequency is between grainDiscardUntil and grainSamplingStartingFrequency, remain all the bids
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

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()
    val prefilteredDS = preFilteringWithPolicy[DailyNegativeSampledBidRequestRecord](dailyNegativeSampledBids, adGroupPolicy, adGroupDS)

    val filterCondition = date_add($"LogEntryTime", $"DataLookBack")>=date
    val dailyNegativeSampledBidsFilterByPolicy = multiLevelJoinWithPolicy[AggregateNegativesRecord](prefilteredDS, adGroupPolicy, filterCondition)

    dailyNegativeSampledBidsFilterByPolicy
  }
}
