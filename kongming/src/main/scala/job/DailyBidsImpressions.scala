package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.loadParquetDataWithHourPart
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingDataset, BidsImpressionsSchema, DailyHourlyBidsImpressionsDataset}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.broadcast
import com.thetradedesk.spark.util.io.FSUtils


object DailyBidsImpressions extends KongmingBaseJob {

  override def jobName: String = "DailyBidsImpressions"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date)
    val campaignList = adGroupPolicyMapping.select("CampaignId").distinct().cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val hoursPerDay: Int = 24
    var rowCounts = new Array[(String, Long)](hoursPerDay)
    (0 until hoursPerDay).par.foreach(hour => {
      val hourPath = s"$bidImpressionsS3Path/year=${date.getYear}/month=${"%02d".format(date.getMonthValue)}/day=${"%02d".format(date.getDayOfMonth)}/hourPart=$hour/"
      if (FSUtils.directoryExists(hourPath)) {
        val hourlyImp = loadParquetDataWithHourPart[BidsImpressionsSchema](bidImpressionsS3Path, date, hour)
        val filteredHourlyImp = hourlyImp.join(broadcast(campaignList), Seq("CampaignId"), "left_semi").selectAs[BidsImpressionsSchema]
        val rowCount = DailyHourlyBidsImpressionsDataset().writePartition(filteredHourlyImp, date, "%02d".format(hour), Some(partCount.HourlyBidsImpressions))
        rowCounts(hour) = rowCount
      } else {
        println(s"[WARN] No BidImpression data found at $hourPath (date=$date, hour=$hour). Skipping this hour.")
        rowCounts(hour) = ("", 0L)
      }
    })

    rowCounts

  }
}
