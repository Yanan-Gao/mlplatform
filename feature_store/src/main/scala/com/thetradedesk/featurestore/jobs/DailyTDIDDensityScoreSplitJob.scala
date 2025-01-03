package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

object DailyTDIDDensityScoreSplitJob extends FeatureStoreBaseJob {

  override val jobName: String = "DailyTDIDDensityScoreSplitJob"
  val jobConfigName: String = "TDIDDensityScoreSplit"

  val salt = "TRM"
  val repartitionNum = 32768

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

  def loadSeedDensity(date: LocalDate) = {
    val dateStr = getDateStr(date)
    spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=SeedId/config=DailySeedDensityScore/v=1/date=${dateStr}")
  }

  def loadTDIDSiteZipMappingRDD(siteZipToTDIDDF: DataFrame, splitIndex: Int, numPartitions: Int) = {
    val saltValues = (0 until numPartitions).map(lit)
    siteZipToTDIDDF
      .filter((col("TDID").substr(9, 1) === lit("-")) && (abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(10) === lit(splitIndex)))
      .select("SiteZipHashed", "TDID")
      .withColumn("random", explode(array(saltValues: _*)))
      .repartition(repartitionNum, 'SiteZipHashed, 'random)
  }

  def loadPolicyTable(date: LocalDate, sources: Int*) = {
    // read the given date's RSM policy table and filter for only seeds
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/${getDateStr(date)}000000/")
      // filter non graph data only
      .filter(col("CrossDeviceVendorId") === lit(CrossDeviceVendor.None.id) && col("Source").isin(sources))
      .select(col("SourceId").as("SeedId"), col("MappingId"), col("SyntheticId"))
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val dateStr = getDateStr(date)
    val numPartitions = 10

    val seedDensity = loadSeedDensity(date)
    val policyTable = loadPolicyTable(date, DataSource.Seed.id, DataSource.TTDOwnData.id)

    val maxDensityScoreAggUDF = udaf(MaxDensityScoreAgg)

    val syntheticIdSiteZipHashedDensityScore = seedDensity.join(policyTable.select("SeedId", "SyntheticId"), Seq("SeedId"))
      .select('SyntheticId, 'SiteZipHashed, 'DensityScore.cast("float").as("DensityScore"), (rand() * lit(numPartitions)).cast("int").as("random"))
      .repartition(repartitionNum, 'SiteZipHashed, 'random)
      .groupBy('SiteZipHashed, 'random)
      .agg(
        arrays_zip(collect_list('SyntheticId).as("SyntheticId"), collect_list('DensityScore).as("DensityScore")).as("SyntheticIdDensityScores")
      )
      .cache()

    val syntheticIdToMappingId =
      spark.sparkContext.broadcast(loadPolicyTable(date, DataSource.Seed.id)
        .select('SyntheticId, 'MappingId.cast("short").as("MappingId"))
        .as[(Int, Short)]
        .collect()
        .toMap)

    val syntheticIdToMappingIdUdf = udf(
      (pairs: Seq[Int], maxLength: Int) =>
        {
          val result = pairs.flatMap(e => syntheticIdToMappingId.value.get(e))
          if (pairs.length <= maxLength) result
          else Random.shuffle(result).take(maxLength)
        }
    )

    val siteZipToTDIDDF = spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/config=DailyTDIDSiteZipMapping/v=1/date=${dateStr}")
      .select("SiteZipHashed", "TDID")
      .cache()

    val overrideOutput = config.getBoolean("overrideOutput", default = false)
    val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

    def processSplit(splitIndex: Int): Unit = {
      val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/config=$jobConfigName/v=1/date=$dateStr/split=$splitIndex"
      val successFile = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/config=$jobConfigName/v=1/date=$dateStr/split=$splitIndex/_SUCCESS"
      if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
        println(s"split ${splitIndex} data is existing")
        return
      }

      val tdidSiteZipHashed = loadTDIDSiteZipMappingRDD(siteZipToTDIDDF, splitIndex, numPartitions)
      val joinedDf = tdidSiteZipHashed.join(syntheticIdSiteZipHashedDensityScore, Seq("SiteZipHashed", "random"))
        .select('TDID, 'SyntheticIdDensityScores)

      val tdidDensityScore = joinedDf
        .groupBy('TDID)
        .agg(maxDensityScoreAggUDF(col("SyntheticIdDensityScores")).as("SyntheticIdDensityScores"))
        .withColumn("SyntheticId_Level2", DensityScoreFilterUDF.apply(0.99f, 1.01f)('SyntheticIdDensityScores))
        .withColumn("MappingIdLevel2", syntheticIdToMappingIdUdf('SyntheticId_Level2, lit(maxNumMappingIdsInAerospike)))
        .withColumn("SyntheticId_Level1", DensityScoreFilterUDF.apply(0.8f, 0.99f)('SyntheticIdDensityScores))
        .withColumn("MappingIdLevel1", syntheticIdToMappingIdUdf('SyntheticId_Level1, lit(maxNumMappingIdsInAerospike) - size('MappingId_Level2)))

      tdidDensityScore.coalesce(16380).write.mode(SaveMode.Overwrite).parquet(writePath)
    }

    splitIndex.foreach(
      processSplit
    )

    syntheticIdSiteZipHashedDensityScore.unpersist()
    siteZipToTDIDDF.unpersist()

    Array(("", 0))
  }
}
