package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyBidRequestDataset, DailyBidsImpressionsDataset}
import com.thetradedesk.kongming.transform.BidRequestTransform

object DailyBidRequest extends KongmingBaseJob {

  override def jobName: String = "DailyBidRequest"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)

    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    val filteredBidRequestDS = BidRequestTransform.dailyTransform(
      bidsImpressionFilterByPolicy,
      adGroupPolicy
    )(getPrometheus)

    val rowCount = DailyBidRequestDataset().writePartition(filteredBidRequestDS, date, Some(partCount.DailyBidRequest))

    Array(rowCount)

  }
}
