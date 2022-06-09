package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.kongming.datasets.DailyOfflineScoringRecord
import com.thetradedesk.kongming.datasets.AdGroupPolicyRecord
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.broadcast
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
    bidsImpressions
      //Assuming ConfigKey will always be adgroupId.
      .join(broadcast(adGroupPolicy), bidsImpressions("AdGroupId") === adGroupPolicy("ConfigValue"))
      .filter($"IsImp" === true)
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
