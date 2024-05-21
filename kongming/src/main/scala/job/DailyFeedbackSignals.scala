package job

import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.defaultCloudProvider
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions._

import java.time.LocalDate

object DailyFeedbackSignals extends KongmingBaseJob {

  override def jobName: String = "DailyFeedbackSignals"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date)
    val minimalPolicy = getMinimalPolicy(adGroupPolicy, adGroupPolicyMapping)

    // daily bids feedbacks
    val bidfeedback = loadParquetData[DailyBidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = date,
      lookBack = Some(0)
    )
    val dailybf = multiLevelJoinWithPolicy[DailyBidFeedbackRecord](bidfeedback, minimalPolicy, joinType = "left_semi")
    val bfRowCount = DailyBidFeedbackDataset().writePartition(dailybf, date, Some(100))

    // daily clicks
    val clicks = ClickTrackerDataSetV5(defaultCloudProvider).readDate(date).selectAs[DailyClickRecord]
    val dailyClicks = multiLevelJoinWithPolicy[DailyClickRecord](clicks, minimalPolicy, joinType = "left_semi")
    val clickRowCount = DailyClickDataset().writePartition(dailyClicks, date, Some(1))

    // daily attributed events and results
    val attributedEvent = AttributedEventDataSet().readDate(date).selectAs[AttributedEventRecord]
    val filteredAttributedEvent = multiLevelJoinWithPolicy[AttributedEventRecord](attributedEvent, minimalPolicy, joinType = "left_semi")
      .filter($"AttributedEventTypeId".isin(List("1", "2"): _*))
      .withColumn("AttributedEventLogEntryTime", to_timestamp(col("AttributedEventLogEntryTime")).as("AttributedEventLogEntryTime"))
      .selectAs[AttributedEventRecord]
    val attributedEventResult = AttributedEventResultDataSet().readDate(date)
      .filter($"AttributionMethodId".isin(List("0", "1"): _*))
      .selectAs[AttributedEventResultRecord]
    val dailyAttribution = filteredAttributedEvent.join(
      attributedEventResult.withColumn("ConversionTrackerLogEntryTime", to_timestamp(col("ConversionTrackerLogEntryTime")).as("ConversionTrackerLogEntryTime")),
      Seq("ConversionTrackerLogFileId", "ConversionTrackerIntId1", "ConversionTrackerIntId2", "AttributedEventLogFileId", "AttributedEventIntId1", "AttributedEventIntId2"),
      "inner"
    ).selectAs[DailyAttributionRecord]
    val attrRowCount = DailyAttributionDataset().writePartition(dailyAttribution, date, Some(1))

    Array(bfRowCount, clickRowCount, attrRowCount)

  }
}
