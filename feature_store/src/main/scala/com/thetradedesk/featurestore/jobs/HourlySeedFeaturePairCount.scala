package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object HourlySeedFeaturePairCount extends DensityFeatureBaseJob {
  override val jobName = "HourlySeedFeaturePairCount"

  override def runTransform(args: Array[String]): Unit = {
    for (i <- hourArray) {
      runTransformHour(args, hourInt = i)
    }
  }

  def runTransformHour(args: Array[String], hourInt: Int): Unit = {
    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/job=${jobName}/v=1/date=${getDateStr(date)}/hour=$hourInt/"
    val successFile = s"$writePath/_SUCCESS"

    // skip processing this split if data from a previous run already exists
    if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
      println(s"split ${splitIndex} data is existing")
      return
    }
    val bidreq = readBidsImpressions(featurePairs, date, Some(hourInt))
    val aggregatedSeed = readAggregatedSeed(date.minusDays(1))
    val policyTable = readPolicyTable(date.minusDays(1), DataSource.Seed.id, DataSource.TTDOwnData.id)

    val aggregatedResult = aggregateFeatureCount(bidreq, aggregatedSeed, policyTable)

    aggregatedResult.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)
  }

  private[jobs]
  def aggregateFeatureCount(bidReq: DataFrame, aggregatedSeed: DataFrame, policyTable: DataFrame): DataFrame = {
    // hourly count per TDID and feature
    val userFeatureCount = featurePairStrings.map { case (featurePair) =>
      bidReq.select(
          col("TDID"),
          col(s"${featurePair}Hashed").as("FeatureValueHashed"),
          lit(featurePair).as("FeatureKey"),
          lit(filterSensitiveAdv(featurePair)).as("IsSensitive")
        )
        .filter(col("FeatureValueHashed").isNotNull)
        .groupBy("TDID", "FeatureValueHashed", "FeatureKey", "IsSensitive")
        .count()
    }.reduce(_ union _)

    // hourly count per seed and feature
    val seedFeatureCount = userFeatureCount
      .join(aggregatedSeed.select("TDID", "SeedIds"), "TDID")
      .drop("TDID")
      .withColumn("SeedId", explode(col("SeedIds")))
      .drop("SeedIds")
      .as("b")
      .join(broadcast(policyTable).as("p"), Seq("SeedId"))
      .filter($"b.IsSensitive" === $"p.IsSensitive" || col("Source") === lit(DataSource.TTDOwnData.id))
      .groupBy("SeedId", "FeatureValueHashed", "FeatureKey")
      .agg(sum("count").as("HourlyCount"))

    seedFeatureCount
  }
}
