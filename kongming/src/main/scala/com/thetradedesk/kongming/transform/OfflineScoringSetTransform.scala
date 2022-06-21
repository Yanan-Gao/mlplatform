package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.kongming
import com.thetradedesk.kongming.datasets.AdGroupDataset
import com.thetradedesk.kongming.datasets.DailyOfflineScoringRecord
import com.thetradedesk.kongming.datasets.AdGroupPolicyRecord
import com.thetradedesk.kongming.datasets.AdGroupRecord
import com.thetradedesk.kongming.multiLevelJoinWithPolicy
import com.thetradedesk.kongming.preFilteringWithPolicy
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit

object OfflineScoringSetTransform {
  val DEFAULT_NUM_PARTITION = 200

  /**
   * Getting policy adgroup's impression data for scoring purpose.
   * @param bidsImpressions bids impression dataset
   * @param adGroupPolicy policy dataset
   * @param selectionTabular column transformations
   * @param prometheus
   * @return
   */
  def dailyTransform(bidsImpressions: Dataset[BidsImpressionsSchema],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord],
                     selectionTabular: Array[Column]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[DailyOfflineScoringRecord] = {

    val adGroupDS = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, kongming.date)
    val prefilteredDS = preFilteringWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, adGroupDS)

    val filterCondition = $"IsImp" === true
    val bidsImpressionsFilterByPolicy = multiLevelJoinWithPolicy[BidsImpressionsSchema](prefilteredDS, adGroupPolicy, filterCondition)

    bidsImpressionsFilterByPolicy
      //Assuming ConfigKey will always be adgroupId.
      .withColumn("AdFormat",concat(col("AdWidthInPixels"),lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .select(selectionTabular: _*)
      .as[DailyOfflineScoringRecord]
  }
}
