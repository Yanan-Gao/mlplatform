package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object HourlySeedSiteZipCount extends FeatureStoreBaseJob {
  override val jobName = "HourlySeedSiteZipCount"
  val jobConfigName = "HourlySeedSiteZipCount"
  val salt = "TRM"

  def loadInputData(date: LocalDate, hourInt: Int = 0) = {
    val dateStr = getDateStr(date)
    val yyyy = dateStr.substring(0, 4)
    val mm = dateStr.substring(4, 6)
    val dd = dateStr.substring(6, 8)

    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/hourPart=$hourInt/")
      .select("UIID", "Site", "Zip")
      .na.drop()
      .withColumnRenamed("UIID", "TDID")
      .filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("SiteZipHashed", xxhash64(concat(concat(col("Site"), col("Zip")), lit(salt))))
  }

  def readLatestAggregatedSeed(): DataFrame = {
    for (i <- 0 to 6) {
      val sourcePath = s"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/aggregatedSeed/v=1/date=${getDateStr(date.minusDays(i))}"
      val sourceSuccessFilePath = s"${sourcePath}/_SUCCESS"

      if (FSUtils.fileExists(sourceSuccessFilePath)(spark)) {
        return spark.read.parquet(sourcePath)
      }
    }
    throw new RuntimeException("aggregated seed dataset not existing")
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    for (i <- hourArray) {
      runTransformHour(args, hourInt = i)
    }
    Array(("", 0))
  }

  def runTransformHour(args: Array[String], hourInt: Int): Unit = {
    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=${jobConfigName}/v=1/date=${getDateStr(date)}/hour=$hourInt/"
    val successFile = s"$writePath/_SUCCESS"

    // skip processing this split if data from a previous run already exists
    if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
      println(s"split ${splitIndex} data is existing")
      return
    }
    val bidreq = loadInputData(date, hourInt)
    val aggregatedSeed = readLatestAggregatedSeed()

    val hourlySeedDensity = bidreq.join(aggregatedSeed.select("TDID", "SeedIds"), "TDID")
      .withColumn("SeedId", explode(col("SeedIds")))
      .drop("SeedIds")
      .groupBy("SeedId", "SiteZipHashed")
      .count()
      .withColumnRenamed("Count", "SeedCount")

    hourlySeedDensity.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)

    println(writePath)
  }
}