package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.loadParquetDataWithHourPart
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingDataset, BidsImpressionsSchema, DailyHourlyBidsImpressionsDataset}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.broadcast


object DailyBidsImpressions extends KongmingBaseJob {

  override def jobName: String = "DailyBidsImpressions"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date)
    val campaignList = adGroupPolicyMapping.select("CampaignId").distinct().cache()

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val hoursPerDay: Int = 24
    var rowCounts = new Array[(String, Long)](hoursPerDay)
    (0 until hoursPerDay).par.foreach(hour => {
      val hourlyImp = loadParquetDataWithHourPart[BidsImpressionsSchema](bidImpressionsS3Path, date, hour)
      val filteredHourlyImp = hourlyImp.join(broadcast(campaignList), Seq("CampaignId"), "left_semi").selectAs[BidsImpressionsSchema]
      val rowCount = DailyHourlyBidsImpressionsDataset().writePartition(filteredHourlyImp, date, "%02d".format(hour), Some(partCount.HourlyBidsImpressions))
      rowCounts(hour) = rowCount
    })

    rowCounts

  }
}
