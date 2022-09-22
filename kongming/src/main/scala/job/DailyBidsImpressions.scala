package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupDataSet, AdGroupPolicyDataset, DailyBidsImpressionsDataset}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object DailyBidsImpressions {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient("KoaV4Conversion", "DailyBidsImpressions")

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)

    val adGroupDS = AdGroupDataSet().readLatestPartitionUpTo(date, true)

    val dailyBidsImpressions = preFilteringWithPolicy(bidsImpressions, adGroupPolicy, adGroupDS)

    DailyBidsImpressionsDataset().writePartition(dailyBidsImpressions, date, Some(400))

    spark.stop()
  }
}
