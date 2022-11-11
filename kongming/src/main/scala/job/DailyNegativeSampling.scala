package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.kongming.transform.NegativeTransform
import com.thetradedesk.kongming.transform.NegativeTransform.NegativeSamplingBidRequestGrainsRecord
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object DailyNegativeSampling {
  /*
    Input: all bids of a day
    Output: downsampled bids per adgroup
    Method: first randomly sample half(TBD) of bids, then sample bids by grain's frequency at adgroup level

    With below parameters, on 2022-03-26, 216 billion -> 5.6 billion bids, all adgroup have bids
    Negative sampling is done at adgroup level, regardless of adgroup policy aggkey.  We'll apply aggkey in next job of aggregation negatives.

   */
  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, "DailyNegativeSampling")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](BidsImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)


    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()
    val prefilteredDS = preFilteringWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, adGroupDS)
    val bidsImpressionFilterByPolicy = multiLevelJoinWithPolicy[BidsImpressionsSchema](prefilteredDS, adGroupPolicy)

    // one day's bidrequest
    val initialBidRequests =bidsImpressionFilterByPolicy
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamily.value")
      .withColumn("Browser", $"Browser.value")
      .selectAs[NegativeSamplingBidRequestGrainsRecord]

    // policy 0: constant down sampling
    val downSamplingConstantMod =  config.getInt("downSamlingConstantMode", 10)
    val downSampledBidRequestWithConstantMod = NegativeTransform
      .samplingWithConstantMod(initialBidRequests, downSamplingConstantMod)(prometheus)

    // policy 1: sample by grain
    val grainsForSampling = Seq(
      "AdGroupId",
      "SupplyVendor",
      "Site",
      "RenderingContext",
      "Country",
      "DeviceType",
      "OperatingSystemFamily",
      "Browser"
    )

    val grainSamplingStartingFrequency =  config.getInt(path="grainSamplingStartingFrequency", 100 )

    val grainDiscardUntil = config.getInt(path="grainDiscardUntil", 30)

    val grainSampleRateSmoother = config.getDouble(path="grainSampleRateSmoother", 0.95)

    val totalBidPenalty = config.getDouble(path="totalBidPenalty", 0.5)

    val downSampledBidRequestByGrain = NegativeTransform
      .samplingByGrains(
        downSampledBidRequestWithConstantMod,
        grainsForSampling,
        grainSamplingStartingFrequency= grainSamplingStartingFrequency,
        grainDiscardUntil = grainDiscardUntil,
        grainSampleRateSmoother = grainSampleRateSmoother,
        totalBidPenalty = totalBidPenalty,
        samplingSeed = samplingSeed
      )(prometheus)
      .toDF()
      .selectAs[DailyNegativeSampledBidRequestRecord]

    val dailyNegRows = DailyNegativeSampledBidRequestDataSet().writePartition(downSampledBidRequestByGrain, date, Some(100))

    outputRowsWrittenGauge.labels("DailyNegativeSampledBidRequest").set(dailyNegRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}