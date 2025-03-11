package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object HourlySeedFeaturePairCount extends DensityFeatureBaseJob {
  override val jobName = "HourlySeedFeaturePairCount"

  override def runTransform(args: Array[String]): Unit = {
    val bidreq = readBidsImpressions(featurePairs, date, Some(hourInt))
    val aggregatedSeed = readAggregatedSeed(date)
    val policyTable = readPolicyTable(date.minusDays(1), DataSource.Seed.id, DataSource.TTDOwnData.id)

    val bidreqWithAggSeed = bidreq.join(aggregatedSeed.select("TDID", "SeedIds"), "TDID")
      .withColumn("SeedId", explode(col("SeedIds")))
      .drop("SeedIds")
      .join(broadcast(policyTable), "SeedId")

    val aggregatedResults = featurePairStrings.map { case (featurePair) =>
      bidreqWithAggSeed
        .filter(col("IsSensitive") === lit(filterSensitiveAdv(featurePair)) && col(s"${featurePair}Hashed").isNotNull)
        .groupBy("SeedId", s"${featurePair}Hashed")
        .count()
        .withColumnRenamed("count", s"HourlyCount")
        .withColumn("FeatureKey", lit(featurePair))
        .withColumnRenamed(s"${featurePair}Hashed", "FeatureValueHashed")
    }

    val reducedDf = aggregatedResults.reduce(_ union _)

    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/job=${jobName}/v=1/date=${getDateStr(date)}/hour=$hourInt/"
    reducedDf.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)
  }
}
