package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object HourlySeedFeaturePairCount extends DensityFeatureBaseJob {
  override val jobName = "HourlySeedFeaturePairCount"

  override def runTransform(args: Array[String]): Unit = {
    for (i <- hourArray) {
      runTransform0(args, hourInt = i)
    }
  }

  def runTransform0(args: Array[String], hourInt: Int): Unit = {
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

    val bidreqWithAggSeed = bidreq.join(aggregatedSeed.select("TDID", "SeedIds"), "TDID")
      .withColumn("SeedId", explode(col("SeedIds")))
      .drop("SeedIds")
      .join(broadcast(policyTable), "SeedId")

    val aggregatedResults = featurePairStrings.map { case (featurePair) =>
      bidreqWithAggSeed
        .filter((col("IsSensitive") === lit(filterSensitiveAdv(featurePair)) || col("Source") === lit(DataSource.TTDOwnData.id)) && col(s"${featurePair}Hashed").isNotNull)
        .groupBy("SeedId", s"${featurePair}Hashed")
        .count()
        .withColumnRenamed("count", s"HourlyCount")
        .withColumn("FeatureKey", lit(featurePair))
        .withColumnRenamed(s"${featurePair}Hashed", "FeatureValueHashed")
    }

    val reducedDf = aggregatedResults.reduce(_ union _)

    reducedDf.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)
  }
}
