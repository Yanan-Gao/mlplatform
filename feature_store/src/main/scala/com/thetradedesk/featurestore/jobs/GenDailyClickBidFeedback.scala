package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.dsDefaultStorageProvider
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast


/** Join daily Click data with BidFeedback
 *
 */
object GenDailyClickBidFeedback extends FeatureStoreGenJob[ClickBidFeedbackRecord] {

  override def initDataSet(): DailyClickBidFeedbackDataset = {
    DailyClickBidFeedbackDataset()
  }

  override def generateDataSet(): Dataset[ClickBidFeedbackRecord] = {
    val bidfeedback = loadParquetData[BidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = date,
      lookBack = Some(0)
    )

    val clicks = ClickTrackerDataSetV5(dsDefaultStorageProvider).readDate(date).select("BidRequestId", "ClickRedirectId")

    val clickBidFeedback = bidfeedback.join(broadcast(clicks), Seq("BidRequestId"), "left")
      .selectAs[ClickBidFeedbackRecord]

    clickBidFeedback
  }
}

