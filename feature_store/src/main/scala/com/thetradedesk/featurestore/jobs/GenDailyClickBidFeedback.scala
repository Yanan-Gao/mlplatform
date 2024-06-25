package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.defaultCloudProvider


/** Join daily Click data with BidFeedback
 *
 */
object GenDailyClickBidFeedback extends FeatureStoreBaseJob {

  override def jobName: String = "genDailyBidFeedback"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidfeedback = loadParquetData[BidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = date,
      lookBack = Some(0)
    )

    val clicks = ClickTrackerDataSetV5(defaultCloudProvider).readDate(date).select("BidRequestId", "ClickRedirectId")

    val clickBidFeedback = bidfeedback.join(clicks, Seq("BidRequestId"), "left")
      .selectAs[ClickBidFeedbackRecord]

    val row = DailyClickBidFeedbackDataset().writePartition(clickBidFeedback, date, Some(partCount.DailyClickBidFeedback))

    Array(row)

  }
}

