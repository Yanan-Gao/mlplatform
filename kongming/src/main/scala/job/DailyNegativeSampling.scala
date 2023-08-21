package job

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

object DailyNegativeSampling extends KongmingBaseJob {
  /*
    Input: all bids of a day
    Output: downsampled bids per adgroup
    Method: first randomly sample half(TBD) of bids, then sample bids by grain's frequency at adgroup level

    With below parameters, on 2022-03-26, 216 billion -> 5.6 billion bids, all adgroup have bids
    Negative sampling is done at adgroup level, regardless of adgroup policy aggkey.  We'll apply aggkey in next job of aggregation negatives.

   */
  override def jobName: String = "DailyNegativeSampling"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    // one day's bidrequest
    val initialBidRequests = bidsImpressionFilterByPolicy
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamily.value")
      .withColumn("Browser", $"Browser.value")
      .selectAs[NegativeSamplingBidRequestGrainsRecord]

    // policy 0: constant down sampling
    val downSamplingConstantMod = config.getInt("downSamplingConstantMode", 10)
    val downSampledBidRequestWithConstantMod = NegativeTransform
      .samplingWithConstantMod(initialBidRequests, downSamplingConstantMod)(getPrometheus)

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

    val grainSampleRateSmoother = config.getDouble(path="grainSampleRateSmoother", 0.5)

    val downSampledBidRequestByGrain = NegativeTransform
      .samplingByGrains(
        downSampledBidRequestWithConstantMod,
        grainsForSampling,
        grainSamplingStartingFrequency= grainSamplingStartingFrequency,
        grainDiscardUntil = grainDiscardUntil,
        grainSampleRateSmoother = grainSampleRateSmoother,
        samplingSeed = samplingSeed
      )(getPrometheus)
      .toDF()
      .selectAs[DailyNegativeSampledBidRequestRecord]

    val dailyNegRows = DailyNegativeSampledBidRequestDataSet().writePartition(downSampledBidRequestByGrain, date, Some(1000))

    Array(dailyNegRows)

  }
}