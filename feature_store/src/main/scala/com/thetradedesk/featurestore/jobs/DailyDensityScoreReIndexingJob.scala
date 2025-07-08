package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform.DensityScoreFilterUDF
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DailyDensityScoreReIndexingJob extends DensityFeatureBaseJob {

  override val jobName: String = "DailyDensityScoreReIndexingJob"

  val numPartitions = 10
  val repartitionNum = 32768

  def readSeedDensity(date: LocalDate) = {
    spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/job=DailySeedDensityScore/v=1/date=${getDateStr(date)}")
  }

  override def runTransform(args: Array[String]) = {
    val dateStr = getDateStr(date)

    val seedDensity = readSeedDensity(date)
    val policyTable = readPolicyTable(date, DataSource.Seed.id, DataSource.TTDOwnData.id)

    val featurePairDensityScoreJoinedDf = seedDensity
      .join(
        broadcast(policyTable.select("SeedId", "SyntheticId")), Seq("SeedId")
      )
      .select(
        'SyntheticId,
        'FeatureKey,
        'FeatureValueHashed,
        'DensityScore.cast("float").as("DensityScore"),
        (rand() * lit(numPartitions)).cast("int").as("random")
      )
      .repartition(repartitionNum, 'FeatureKey, 'FeatureValueHashed, 'random)
      .groupBy('FeatureKey, 'FeatureValueHashed, 'random)
      .agg(
        arrays_zip(
          collect_list('SyntheticId).as("SyntheticId"),
          collect_list('DensityScore).as("DensityScore")
        ).as("SyntheticIdDensityScores")
      )

    val featurePairDensityScoreCategorized = featurePairDensityScoreJoinedDf
      .groupBy('FeatureKey, 'FeatureValueHashed)
      .agg(flatten(collect_list("SyntheticIdDensityScores")).as("SyntheticIdDensityScores"))
      .withColumn("SyntheticIdLevel2", DensityScoreFilterUDF.apply(0.99f, 1.01f)('SyntheticIdDensityScores))
      .withColumn("SyntheticIdLevel1", DensityScoreFilterUDF.apply(0.8f, 0.99f)('SyntheticIdDensityScores))
      .filter(
        !(size(col("SyntheticIdLevel1")) === lit(0) &&
          size(col("SyntheticIdLevel2")) === lit(0))
      )
      .select("FeatureKey", "FeatureValueHashed", "SyntheticIdLevel1", "SyntheticIdLevel2")

    // joined dataset for downstream processing in rest of the TDID splits
    val writePathBase = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=FeatureKeyValue/job=$jobName"
    val writePathJoinedDf = s"${writePathBase}/config=SyntheticIdDensityScorePolicyTableJoined/date=$dateStr"
    featurePairDensityScoreJoinedDf.coalesce(8192).write.mode(SaveMode.Overwrite).parquet(writePathJoinedDf)

    // categorized dataset for offline model score processing
    val writePathCategorized = s"${writePathBase}/config=SyntheticIdDensityScoreCategorized/date=$dateStr"
    featurePairDensityScoreCategorized.coalesce(8192).write.mode(SaveMode.Overwrite).parquet(writePathCategorized)
  }
}
