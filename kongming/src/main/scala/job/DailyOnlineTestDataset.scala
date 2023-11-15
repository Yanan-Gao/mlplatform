package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider, environment}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.ProdTesting
import org.apache.spark.sql.functions._

import java.time.LocalDate

object DailyOnlineTestDataset extends KongmingBaseJob {

  override def jobName: String = "DailyOnlineTestDataset"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    environment = ProdTesting

    val startDate = config.getDate("startDate" , LocalDate.of(2023,10,16))
    val endDate = config.getDate("endDate", LocalDate.now())

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)

    // daily bids impressions
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))
    val dailyBidsImpressions = multiLevelJoinWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, joinType = "left_semi")
    val impRowCount = DailyBidsImpressionsDataset().writePartition(dailyBidsImpressions, date, Some(partCount.DailyBidsImpressions))

    // daily bids feedbacks
    val bidfeedback = loadParquetData[DailyBidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = date,
      lookBack = Some(0)
    )
    val dailybf = multiLevelJoinWithPolicy[DailyBidFeedbackRecord](bidfeedback, adGroupPolicy, joinType = "left_semi")
    val bfRowCount = DailyBidFeedbackDataset().writePartition(dailybf, date, Some(100))

    // daily clicks
    val clicks = ClickTrackerDataSetV5(defaultCloudProvider).readDate(date).selectAs[DailyClickRecord]
    val dailyClicks = multiLevelJoinWithPolicy[DailyClickRecord](clicks, adGroupPolicy, joinType = "left_semi")
    val clickRowCount = DailyClickDataset().writePartition(dailyClicks, date, Some(10))

    // daily attributed events and results
    val attributedEvent = AttributedEventDataSet().readDate(date).selectAs[AttributedEventRecord]
    val filteredAttributedEvent = multiLevelJoinWithPolicy[AttributedEventRecord](attributedEvent, adGroupPolicy, joinType = "left_semi")
      .filter($"AttributedEventTypeId".isin(List("1", "2"): _*))
      .withColumn("AttributedEventLogEntryTime", to_timestamp(col("AttributedEventLogEntryTime")).as("AttributedEventLogEntryTime"))
      .filter(to_date($"AttributedEventLogEntryTime") >= lit(startDate))
      .filter(to_date($"AttributedEventLogEntryTime") <= lit(endDate))
      .selectAs[AttributedEventRecord]
    val attributedEventResult = AttributedEventResultDataSet().readDate(date)
      .filter($"AttributionMethodId".isin(List("0", "1", "2"): _*))
      .selectAs[AttributedEventResultRecord]
    val dailyAttribution = filteredAttributedEvent.join(
      attributedEventResult.withColumn("ConversionTrackerLogEntryTime", to_timestamp(col("ConversionTrackerLogEntryTime")).as("ConversionTrackerLogEntryTime")),
      Seq("ConversionTrackerLogFileId", "ConversionTrackerIntId1", "ConversionTrackerIntId2", "AttributedEventLogFileId", "AttributedEventIntId1", "AttributedEventIntId2"),
      "inner"
    ).selectAs[DailyAttributionRecord]
    val attrRowCount = DailyAttributionDataset().writePartition(dailyAttribution, date, Some(1))

    Array(impRowCount, bfRowCount, clickRowCount, attrRowCount)

  }
}
