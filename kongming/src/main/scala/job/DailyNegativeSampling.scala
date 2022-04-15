package job

import java.time.LocalDate

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.NegativeTransform._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyNegativeSampledBidRequestDataSet, DailyNegativeSampledBidRequestRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.broadcast
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
    val prometheus = new PrometheusClient("KoaV4Conversion", "DailyNegativeSampling")

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    // test only adgroups in the policy table. since aggKey are all adgroupId, we filter by adgroup id
    val adGroupPolicyHardCodedDate = LocalDate.parse("2022-03-15")
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)
      .select($"ConfigValue".alias("AdGroupId"))

    // one day's bidrequest
    val initialBidRequests =bidsImpressions
      .join(broadcast(adGroupPolicy), Seq("AdGroupId"),"inner")
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

    val normalizedFrequencyThreshold =  config.getDouble(path="normFreqThreshold", 0.0001 )
    val tIndex = config.getDouble(path="tIndex", 0.85)
    val minimumFrequencyPerGrain = config.getInt(path="minFrequencyPerGrain", 5)

    val downSampledBidRequestByGrain = NegativeTransform
      .samplingByGrains(
        downSampledBidRequestWithConstantMod,
        grainsForSampling,
        normalizedFrequencyThreshold= normalizedFrequencyThreshold,
        tIndex = tIndex,
        minimumFrequencyPerGrain = minimumFrequencyPerGrain
      )(prometheus)
      .toDF()
      .selectAs[DailyNegativeSampledBidRequestRecord]

    DailyNegativeSampledBidRequestDataSet.writePartition(downSampledBidRequestByGrain, date)
  }
}
