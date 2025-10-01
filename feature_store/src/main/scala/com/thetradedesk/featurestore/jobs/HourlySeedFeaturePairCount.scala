package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.featurestore.transform.{BatchSeedCountAgg, SeedMergerAgg}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object HourlySeedFeaturePairCount extends DensityFeatureBaseJob {
  override val jobName = "HourlySeedFeaturePairCount"

  override def runTransform(args: Array[String]): Unit = {
    for (i <- hourArray) {
      runTransformHour(args, hourInt = i)
    }
  }

  def runTransformHour(args: Array[String], hourInt: Int): Unit = {
    val writePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=SeedId/job=${jobName}/v=1/date=${getDateStr(date)}/hour=$hourInt/"
    val successFile = s"$writePath/_SUCCESS"

    // skip processing this split if data from a previous run already exists
    if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
      println(s"split ${splitIndex} data is existing")
      return
    }
    val bidreq = readBidsImpressionsWithIDExploded(featurePairs, date, Some(hourInt))
    val policyTable = readPolicyTable(date, DataSource.Seed.id, DataSource.TTDOwnData.id)
    val seedsToExtend = spark.sparkContext.broadcast(
      policyTable.filter('NeedGraphExtension)
        .select('SeedId)
        .distinct()
        .as[String]
        .collect()
        .toSet
    )
    val aggregatedSeed = if (enableSeedExtension) readAggregatedSeed(date, seedsToExtend) else readAggregatedSeed(date)

    val hourlyAggregatedFeatureCount = aggregateFeatureCount(bidreq, aggregatedSeed, policyTable)

    // hack to 8192, change back later
    hourlyAggregatedFeatureCount.repartition(8192).write.mode(SaveMode.Overwrite).parquet(writePath)
  }

  def aggregateFeatureCount(bidreq: DataFrame, aggregatedSeed: DataFrame, policyTable: DataFrame): DataFrame = {
    val seedToSensitiveMap = spark.sparkContext.broadcast(policyTable.filter(col("Source") === lit(DataSource.Seed.id))
      .select('SeedId, 'IsSensitive)
      .as[(String, Boolean)]
      .collect()
      .toMap)

    val seedFilterUDF = udf(
      (needSensitive: Boolean, seedIds: Seq[String]) => {
        val seedToSensitiveMapValue = seedToSensitiveMap.value
        seedIds.filter(
          e => {
            val sensitiveOption = seedToSensitiveMapValue.get(e)
            sensitiveOption.isEmpty || sensitiveOption.get == needSensitive
          }
        )
      }
    )

    val batchCountAgg = udaf(BatchSeedCountAgg)
    val seedMergerAgg = udaf(SeedMergerAgg(policyTable.select('SeedId).as[String].collect()))

    val aggBidReq = bidreq
      .join(aggregatedSeed, "TDID")
      // for each BidRequestId, only count at most once
      .groupBy(featurePairStrings.map(featurePair => col(s"${featurePair}Hashed")) :+ col("BidRequestId"): _*)
      .agg(
        seedMergerAgg('SeedIds).as("SeedIds")
      )

    val aggregatedResults = featurePairStrings.map { case (featurePair) =>
      aggBidReq.withColumnRenamed(s"${featurePair}Hashed", "FeatureValueHashed")
        .filter(col("FeatureValueHashed").isNotNull)
        .withColumn("SeedIds", seedFilterUDF(lit(filterSensitiveAdv(featurePair)), 'SeedIds))
        .groupBy("FeatureValueHashed")
        .agg(
          batchCountAgg('SeedIds).as("BatchCounts")
        )
        .withColumn("BatchCount", explode('BatchCounts))
        .select('FeatureValueHashed, col("BatchCount._1").as("SeedId"), col("BatchCount._2").as("HourlyCount"))
        .withColumn("FeatureKey", lit(featurePair))
    }

    aggregatedResults.reduce(_ union _)
  }
}