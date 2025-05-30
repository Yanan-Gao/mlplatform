package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DailyTDIDSiteZipMapping extends FeatureStoreBaseJob {

  override val jobName: String = "DailyTDIDSiteZipMapping"
  val jobConfigName: String = "DailyTDIDSiteZipMapping"

  val salt = "TRM"

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

  def loadSeedDensity(date: LocalDate) = {
    val dateStr = getDateStr(date)
    spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=DailySeedDensity/v=1/date=${dateStr}")
  }

  def loadPolicyTable(date: LocalDate) = {
    // read the given date's RSM policy table and filter for only seeds
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/${getDateStr(date)}000000/")
      .filter(col("Source") === lit(DataSource.Seed.id))
      .withColumnRenamed("SourceId", "SeedId")
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val dateStr = getDateStr(date)
    val bidreq = loadInputData(date)

    val tdidSiteZipMapping = bidreq.select("TDID", "SiteZipHashed")
      .distinct()

    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/config=$jobConfigName/v=1/date=$dateStr/"

    tdidSiteZipMapping.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)

    Array(("", 0))
  }
}
