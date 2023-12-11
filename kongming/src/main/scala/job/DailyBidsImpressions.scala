package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, AdGroupPolicyMappingDataset, DailyBidsImpressionsFullDataset}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object DailyBidsImpressions extends KongmingBaseJob {

  override def jobName: String = "DailyBidsImpressions"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val adGroupMapping = AdGroupPolicyMappingDataset().readDate(date)
    val policy = getMinimalPolicy(adGroupPolicy, adGroupMapping).cache()

    val dailyBidsImpressions = multiLevelJoinWithPolicy[BidsImpressionsSchema](bidsImpressions, policy, joinType = "left_semi")

    val rowCount = DailyBidsImpressionsFullDataset().writePartition(dailyBidsImpressions, date, Some(partCount.DailyBidsImpressions))

    Array(rowCount)

  }
}
