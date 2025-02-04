package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

object DailySyntheticIdDensityScorePolicyTableJoinJob extends FeatureStoreBaseJob {

  override val jobName: String = "DailySyntheticIdDensityScorePolicyTableJoinJob"
  val jobConfigName: String = "SyntheticIdDensityScorePolicyTableJoined"

  val salt = "TRM"
  val repartitionNum = 32768

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def loadSeedDensity(date: LocalDate) = {
    val dateStr = getDateStr(date)
    spark.read.parquet(s"$MLPlatformS3Root/$readEnv/profiles/source=bidsimpression/index=SeedId/config=DailySeedDensityScore/v=1/date=${dateStr}")
  }

  def loadPolicyTable(date: LocalDate, sources: Int*) = {
    // read the given date's RSM policy table and filter for only seeds
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/configdata/$readEnv/audience/policyTable/RSM/v=1/${getDateStr(date)}000000/")
      // filter non graph data only
      .filter(col("CrossDeviceVendorId") === lit(CrossDeviceVendor.None.id) && col("Source").isin(sources: _*))
      .select(col("SourceId").as("SeedId"), col("MappingId"), col("SyntheticId"))
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val dateStr = getDateStr(date)
    val numPartitions = 10

    val seedDensity = loadSeedDensity(date)
    val policyTable = loadPolicyTable(date, DataSource.Seed.id, DataSource.TTDOwnData.id)

    val siteZipDensityScoreDf = seedDensity.join(policyTable.select("SeedId", "SyntheticId"), Seq("SeedId"))
      .select('SyntheticId, 'SiteZipHashed, 'DensityScore.cast("float").as("DensityScore"), (rand() * lit(numPartitions)).cast("int").as("random"))
      .repartition(repartitionNum, 'SiteZipHashed, 'random)
      .groupBy('SiteZipHashed, 'random)
      .agg(
        arrays_zip(collect_list('SyntheticId).as("SyntheticId"), collect_list('DensityScore).as("DensityScore")).as("SyntheticIdDensityScores")
      )

    // save this to be used in downstream processing in rest of the TDID splits
    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SiteZipHashed/config=SyntheticIdDensityScorePolicyTableJoined/date=$dateStr"

    siteZipDensityScoreDf.write.mode(SaveMode.Overwrite).parquet(writePath)

    Array(("", 0))
  }
}
