package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyOfflineScoringRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.kongming.{multiLevelJoinWithPolicy, preFilteringWithPolicy}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, concat, lit}

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

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()
    val prefilteredDS = preFilteringWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, adGroupDS)

    val filterCondition = $"IsImp" === true
    val bidsImpressionsFilterByPolicy = multiLevelJoinWithPolicy[BidsImpressionsSchema](prefilteredDS, adGroupPolicy, filterCondition, joinType = "left_semi")

    val bidsImp = bidsImpressionsFilterByPolicy
      //Assuming ConfigKey will always be adgroupId.
      .withColumn("AdFormat",concat(col("AdWidthInPixels"),lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")

    val bidsImpContextual = ContextualTransform.generateContextualFeatureTier1(
      bidsImp.select("BidRequestId","ContextualCategories")
        .dropDuplicates("BidRequestId").selectAs[ContextualData]
    )

    bidsImp
      .join(bidsImpContextual, Seq("BidRequestId"), "left")
      .select(selectionTabular: _*)
      .as[DailyOfflineScoringRecord]
  }
}
