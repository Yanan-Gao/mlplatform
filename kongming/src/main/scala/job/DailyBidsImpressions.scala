package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, BidsImpressionsSchema, DailyBidsImpressionsDataset}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object DailyBidsImpressions extends KongmingBaseJob {

  override def jobName: String = "DailyBidsImpressions"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val dailyBidsImpressions = multiLevelJoinWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, joinType = "left_semi")

    val rowCount = DailyBidsImpressionsDataset().writePartition(dailyBidsImpressions, date, Some(10000))

    Array(rowCount)

  }
}
