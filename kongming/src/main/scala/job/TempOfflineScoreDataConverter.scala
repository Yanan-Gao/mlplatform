package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyBidRequestDataset, DailyBidsImpressionsDataset, OldDailyOfflineScoringDataset, OldDailyOfflineScoringParquetDataset, OldDailyOfflineScoringRecord}
import com.thetradedesk.kongming.transform.BidRequestTransform
import com.thetradedesk.spark.util.TTDConfig.config
import job.DailyOfflineScoringSet.partitionCount

object TempOfflineScoreDataConverter extends KongmingBaseJob {

  override def jobName: String = "TempOfflineScoreDataConverter"
  val partitionCount = config.getInt("partitionCount", partCount.DailyOfflineScoring)

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val tfRecordData = OldDailyOfflineScoringDataset().readDate(date)
    val parquetOfflineScoringRows = OldDailyOfflineScoringParquetDataset().writePartition(tfRecordData, date, Some(partitionCount))

    Array(parquetOfflineScoringRows)

  }
}
