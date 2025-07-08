package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.jobs.DailyNewSeedFeaturePairDensityScore.densityFeatureScoreNewSeedMetadataPrefix
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object DailySeedDensityScore extends DensityFeatureBaseJob {

  override val jobName: String = "DailySeedDensityScore"
  val newSeedJobConfigName: String = config.getString("newSeedJobConfigName", "DailyNewSeedDensityScore")
  def readHourlySeedCounts(date: LocalDate, windowSizeDays: Int) = {
    val hourlySeedCountsS3Path = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/job=HourlySeedFeaturePairCount/v=1"
    spark
      .read
      .option("basePath", hourlySeedCountsS3Path)
      .parquet((0 until windowSizeDays)
        .map(e => getDateStr(date.minusDays(e)))
        .map(e => s"${hourlySeedCountsS3Path}/date=$e/"): _*
      )
  }

  override def runTransform(args: Array[String]): Unit = {
    val featurePairs: List[(String, String)] = List(("AliasedSupplyPublisherId", "City"), ("Site", "Zip"))

    val dateStr = getDateStr(date)
    val bidreq = readBidsImpressions(featurePairs, date, None)
    val countDf = readHourlySeedCounts(date, densityFeatureWindowSizeDays)

    val inSeedDensity = countDf.groupBy("SeedId", "FeatureKey", "FeatureValueHashed")
      .agg(sum("HourlyCount").alias("DailyCount"))
      .drop("HourlyCount")
      .withColumn("DailyTotalCount", sum(s"DailyCount").over(Window.partitionBy("SeedId", "FeatureKey")))
      .withColumn("InDensity", col("DailyCount") / col("DailyTotalCount"))

    val populationFreqMapsByFeatureKey = featurePairStrings.map { featurePair =>
      val hashedCol = s"${featurePair}Hashed"

      bidreq
        .select(hashedCol)
        .filter(col(hashedCol).isNotNull)
        .repartition(col(hashedCol))
        .groupBy(hashedCol)
        .agg(
          count("*").alias("PopulationCount")
        )
        .withColumn("TotalPopulationCount", sum("PopulationCount").over())
        .withColumnRenamed(hashedCol, "FeatureValueHashed")
        .withColumn("FeatureKey", lit(featurePair))
    }

    val populationFreqMap = populationFreqMapsByFeatureKey.reduce(_ union _)

    val seedDensityScore = populationFreqMap.join(inSeedDensity, Seq("FeatureKey", "FeatureValueHashed"))
      .withColumn("OutDensity", (col("PopulationCount") - col("DailyCount")) / (col("TotalPopulationCount") - col("DailyTotalCount")))
      .withColumn("DensityScore", col("InDensity") / (col("InDensity") + col("OutDensity")))
      .select("FeatureKey", "FeatureValueHashed", "SeedId", "DensityScore")
      .as[DailySeedDensityScoreEntity]

    // union new seed densityScore
    val densityScores = featurePairStrings.map { featurePair =>
      val siteZipScoreNewSeedPathPreFix = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=$newSeedJobConfigName$featurePair/v=1"
      val siteZipScoreNewSeedPath = s"${siteZipScoreNewSeedPathPreFix}/date=${dateStr}"
      val siteZipScoreNewSeedMetaPath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/metadata/newSeed$featurePair/v=1/date=${dateStr}"
      if (FSUtils.fileExists(siteZipScoreNewSeedMetaPath + "/_EMPTY")(spark) || FSUtils.fileExists(siteZipScoreNewSeedMetaPath + "/_OVER_MAX")(spark)) {
        spark.emptyDataset[DailySeedDensityScoreEntity]
      } else {
        spark.read.parquet(siteZipScoreNewSeedPath).withColumn("FeatureKey", lit(featurePair))
          .select("FeatureKey", "FeatureValueHashed", "SeedId", "DensityScore")
          .as[DailySeedDensityScoreEntity]
      }
    }

    val result = densityScores.reduce(_.union(_)).union(seedDensityScore)

    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/job=$jobName/v=1/date=${dateStr}/"
    result.coalesce(8192).write.mode(SaveMode.Overwrite).parquet(writePath)
  }


}
