package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DailySeedSiteZipDensityScore extends FeatureStoreBaseJob {

  override val jobName: String = "DailySeedSiteZipDensityScore"
  val jobConfigName: String = "DailySeedDensityScore"
  val salt = "TRM"

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def loadInputData(date: LocalDate) = {
    val dateStr = getDateStr(date)
    val yyyy = dateStr.substring(0, 4)
    val mm = dateStr.substring(4, 6)
    val dd = dateStr.substring(6, 8)

    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/")
      .select("UIID", "Site", "Zip")
      .na.drop()
      .withColumnRenamed("UIID", "TDID")
      .filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("SiteZipHashed", xxhash64(concat(concat(col("Site"), col("Zip")), lit(salt))))
  }

  // load hourly seed counts for last 7 days
  def loadHourlySeedCounts(date: LocalDate) = {
    spark
      .read
      .option("basePath", s"s3://thetradedesk-mlplatform-us-east-1/features/feature_store/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=HourlySeedSiteZipCount/v=1/")
      .parquet((0 until  config.getInt("maxNumMappingIdsInAerospike", default = 1))
        .map(e => getDateStr(date.minusDays(e)))
        .map(e => s"s3://thetradedesk-mlplatform-us-east-1/features/feature_store/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=HourlySeedSiteZipCount/v=1/date=$e/"): _*
      )
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val dateStr = getDateStr(date)

    val bidreq = loadInputData(date)

    val hourlySeedCounts = loadHourlySeedCounts(date)

    val seedDensity = hourlySeedCounts.groupBy("SeedId", "SiteZipHashed")
      .agg(sum("SeedCount").alias("Sum"))
      .drop("SeedCount")
      .withColumnRenamed("Sum", "SeedSiteZipCount")
      .withColumn("SeedTotalCount", sum("SeedSiteZipCount").over(Window.partitionBy("SeedId")))
      .withColumn("InDensity", col("SeedSiteZipCount") / col("SeedTotalCount"))

    val generalPopulationFrequencyMap = bidreq.groupBy("SiteZipHashed").count()
      .withColumnRenamed("Count", "PopSiteZipCount")

    val popTotalCount = generalPopulationFrequencyMap.agg(sum("PopSiteZipCount")).first().getLong(0)

    val siteZipScoreSeedId = generalPopulationFrequencyMap
      .join(seedDensity, Seq("SiteZipHashed"))
      .withColumn("OutDensity", (col("PopSiteZipCount") - col("SeedSiteZipCount")) / (lit(popTotalCount) - col("SeedTotalCount")))
      .withColumn("DensityScore", col("InDensity") / (col("InDensity") + col("OutDensity")))

    val siteZipScoreWritePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=$jobConfigName/v=1/date=${dateStr}"

    siteZipScoreSeedId.repartition(defaultNumPartitions, col("SiteZipHashed")).write.mode(SaveMode.Overwrite).parquet(siteZipScoreWritePath)

    Array(("", 0))
  }
}
