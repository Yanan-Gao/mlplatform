package com.thetradedesk.audience.utils

import com.thetradedesk.audience.datasets.{CampaignFlightDataSet, CampaignSeedDataset}
import com.thetradedesk.audience.{date, dateTime}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SeedListUtils {

  def activeCampaignSeedAndIdFilterUDF(): (DataFrame, UserDefinedFunction) = {
    val activeCampaignSeed = CampaignSeedDataset()
      .readPartition(dateTime.toLocalDate)
      .join(CampaignFlightDataSet.activeCampaigns(date, startShift = 2, endShift = -1), Seq("CampaignId"))
      .cache()

    val activeSeedIdsValue = spark.sparkContext.broadcast(
      activeCampaignSeed
        .select("SeedId")
        .distinct()
        .as[String]
        .collect()
        .toSet
    )

    (activeCampaignSeed, seedIdFilterUDF(activeSeedIdsValue))
  }

  def seedIdFilterUDF(selectedSeedIds: Broadcast[Set[String]]): UserDefinedFunction = {
    udf((seedIds: Seq[String]) => {
      val activeSeedIds = selectedSeedIds.value
      seedIds.filter(activeSeedIds.contains)
    })
  }
}
