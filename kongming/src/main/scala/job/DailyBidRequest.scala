package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyBidRequestDataset}
import com.thetradedesk.kongming.transform.BidRequestTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate

object DailyBidRequest {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient("KoaV4Conversion", "DailyBidRequest")

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)

    val filteredBidRequestDS = BidRequestTransform.dailyTransform(
      bidsImpressions,
      adGroupPolicy
    )(prometheus)

    DailyBidRequestDataset.writePartition(filteredBidRequestDS, date)

  }
}
