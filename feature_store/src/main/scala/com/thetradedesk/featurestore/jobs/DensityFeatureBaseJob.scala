package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.CrossDeviceVendor
import com.thetradedesk.featurestore.transform.IDTransform.{allIdType, filterOnIdTypes}
import com.thetradedesk.featurestore.transform.MappingIdSplitUDF
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.provisioning.CampaignFlightDataSet
import com.thetradedesk.spark.datasets.sources.{CampaignSeedDataSet, SeedDetailDataSet}
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

abstract class DensityFeatureBaseJob {
  val jobName = "DensityFeatureBaseJob"

  // configuration overrides
  val policyEnv: String = config.getString("policyEnv", readEnv)
  val aggSeedEnv: String = config.getString("aggSeedEnv", readEnv)
  val tdidDensityFeatureEnv: String = config.getString("tdidDensityFeatureEnv", ttdEnv)


  val nonSensitiveFeaturePair = "SiteZip"
  val sensitiveFeaturePair = "AliasedSupplyPublisherIdCity"
  val featurePairs: List[(String, String)] = List(("AliasedSupplyPublisherId", "City"), ("Site", "Zip"))

  val featurePairStrings = featurePairs.map { case (f1, f2) => s"$f1$f2" }

  val numSplits = 10
  val salt = "TRM"

  val filterSensitiveAdv: mutable.HashMap[String, Boolean] = mutable.HashMap[String, Boolean](
    sensitiveFeaturePair -> true,
    nonSensitiveFeaturePair -> false
  )

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def readBidsImpressionsWithIDExploded(
                                         featurePairs: List[(String, String)],
                                         date: LocalDate,
                                         hour: Option[Int]
                                       ): DataFrame = {
    val userWindowSpec = if (hour.isDefined) Window.partitionBy("TDID") else Window.partitionBy("TDID", "hourPart")

    readBidsImpressions(featurePairs, date, hour)
      .withColumn("TDID", allIdType)
      .withColumn("UserFrequency", count("*").over(userWindowSpec))
      .where(col("UserFrequency") < lit(normalUserBidCountPerHour))
      .drop("UserFrequency")
      .select("TDID", (featurePairs.map(e => s"${e._1}${e._2}Hashed") :+ "BidRequestId"): _*)
  }

  def readBidsImpressions(featurePairs: List[(String, String)], date: LocalDate, hour: Option[Int]) = {
    val yyyy = date.getYear.toString
    val mm = f"${date.getMonthValue}%02d"
    val dd = f"${date.getDayOfMonth}%02d"

    val colsToRead = featurePairs.flatMap { case (f1, f2) => Array(f1, f2) }.toArray.distinct
    val userWindowSpec = Window.partitionBy("UIID")
    val hourUserWindowSpec = Window.partitionBy("UIID", "hourPart")

    try {
      val bidsImpsPath = s"${BidsImpressions.BIDSIMPRESSIONSS3}/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/"

      var bidsImpressions = hour match {
        case Some(h) => spark.read.parquet(bidsImpsPath + s"hourPart=$h/")
          .withColumn("UserFrequency", count("*").over(userWindowSpec))
          .where('UserFrequency < lit(normalUserBidCountPerHour))
          .drop('UserFrequency)
        case _ => spark.read.parquet(bidsImpsPath)
          .withColumn("UserFrequency", count("*").over(hourUserWindowSpec))
          .where('UserFrequency < lit(normalUserBidCountPerHour))
          .drop('UserFrequency)
      }

      val emptyFilter = featurePairs.map(e => (col(e._1).isNotNull && col(e._2).isNotNull)).reduce(_ || _)

      bidsImpressions = bidsImpressions
        .where(emptyFilter)
        .where(filterOnIdTypes(shouldTrackTDID))

      featurePairs.foreach { case (feature1, feature2) =>
        bidsImpressions = bidsImpressions.withColumn(
          s"${feature1}${feature2}Hashed",
          when(
            col(feature1).isNull || col(feature2).isNull,
            lit(null)
          ).otherwise(
            xxhash64(concat(concat(col(feature1), col(feature2)), lit(salt)))
          )
        )
      }

      val baseCols = colsToRead ++ featurePairs.map(e => s"${e._1}${e._2}Hashed") ++ 
        Seq("DeviceAdvertisingId", "CookieTDID", "UnifiedId2", "EUID", "IdentityLinkId")
      
      val colsToKeep = if (hour.isDefined) baseCols else baseCols :+ "hourPart"

      bidsImpressions
        .select("BidRequestId", colsToKeep: _*)
    }
    catch {
      case e: Throwable =>
        println("Bad column names. Please provide a valid job config!")
        throw (e)
    }
  }

  def readPolicyTable(date: LocalDate, sources: Int*) = {
    // read the given date's RSM policy table and filter for only seeds
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/configdata/$policyEnv/audience/policyTable/RSM/v=1/${getDateStr(date)}000000/")
      // filter non graph data only
      .filter(col("CrossDeviceVendorId") === lit(CrossDeviceVendor.None.id) && col("Source").isin(sources: _*))
      .select(col("SourceId").as("SeedId"), col("MappingId"), col("SyntheticId"), col("IsSensitive"), col("NeedGraphExtension"), 'Source)
  }

  def readAggregatedSeed(date: LocalDate): DataFrame = {
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/data/$aggSeedEnv/audience/aggregatedSeed/v=1/date=${getDateStr(date)}")
  }

  def readAggregatedSeed(date: LocalDate, extendableSeedsBroadcast: Broadcast[Set[String]]): DataFrame = {
    val seedFilterUDF = udf(
      (seedIds: Seq[String]) => {
        val extendableSeeds = extendableSeedsBroadcast.value
        seedIds.filter(
          e => {
            extendableSeeds.contains(e)
          }
        )
      }
    )

    spark.read
      .parquet(s"s3://thetradedesk-mlplatform-us-east-1/data/$aggSeedEnv/audience/aggregatedSeed/v=1/date=${getDateStr(date)}")
      .withColumn("PersonGraphSeedIds", seedFilterUDF('PersonGraphSeedIds))
      .withColumn("SeedIds", array_union(
        coalesce(col("SeedIds"), array()),
        coalesce(col("PersonGraphSeedIds"), array())
      ))
      .select("TDID", "SeedIds")
  }

  def readActiveCampaigns(date: LocalDate): DataFrame = {
    val campaignStartDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(campaignFlightStartingBufferInDays))
    val campaignEndDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(campaignFlightEndingBufferInDays))
    CampaignFlightDataSet()
      .readLatestPartition()
      .where('IsDeleted === false
        && 'StartDateInclusiveUTC.leq(campaignStartDateTimeStr)
        && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(campaignEndDateTimeStr)))
      .select('CampaignId)
      .distinct()
  }

  def readTDIDDensityFeature(date: LocalDate): DataFrame = {
    val basePath = s"$MLPlatformS3Root/$tdidDensityFeatureEnv/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=${getDateStr(date)}/"
    readAllSplits(date, basePath)
  }

  def readTDIDDensityFeature(date: LocalDate, splitNum: Int): DataFrame = {
    val basePath = s"$MLPlatformS3Root/$tdidDensityFeatureEnv/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=${getDateStr(date)}/"
    spark
      .read
      .option("basePath", basePath)
      .parquet(s"$basePath/split=$splitNum")
  }

  def readAllSplits(date: LocalDate, basePath: String): DataFrame = {
    spark
      .read
      .option("basePath", basePath)
      .parquet(((0 until 10).map(e => s"$basePath/split=$e")): _*)
  }

  def splitDensityScoreSlots(input: DataFrame): DataFrame = {
    input
      .withColumn("MappingIdLevel2S1", MappingIdSplitUDF(1)('MappingIdLevel2))
      .withColumn("MappingIdLevel2", MappingIdSplitUDF(0)('MappingIdLevel2))
      .withColumn("MappingIdLevel1S1", MappingIdSplitUDF(1)('MappingIdLevel1))
      .withColumn("MappingIdLevel1", MappingIdSplitUDF(0)('MappingIdLevel1))
  }

  def getSeedPriority: DataFrame = {

    // Get active seed ids
    val activeCampaign = readActiveCampaigns(date)

    val newSeedStartDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.minusDays(newSeedBufferInDays))

    val newSeed = SeedDetailDataSet()
      .readLatestPartition()
      .where(to_timestamp('CreatedAt, "yyyy-MM-dd HH:mm:ss").gt(newSeedStartDateTimeStr))
      .select('SeedId)
      .withColumn("SeedPriority", lit(SeedPriority.NewSeed.id))

    val campaignSeed = CampaignSeedDataSet()
      .readLatestPartition()

    // active campaign seed >> new seed >> inactive campaign seed >> other
    campaignSeed
      .join(activeCampaign.withColumn("SeedPriority", lit(SeedPriority.ActiveCampaignSeed.id)), Seq("CampaignId"), "left")
      .withColumn("SeedPriority", coalesce('SeedPriority, lit(SeedPriority.CampaignSeed.id)))
      .groupBy('SeedId)
      .agg(
        max('SeedPriority).as("SeedPriority")
      )
      .unionByName(newSeed)
      .groupBy('SeedId)
      .agg(
        max('SeedPriority).as("SeedPriority")
      )
  }

  def getSyntheticIdToMappingId(policyData: DataFrame, seedPriority: DataFrame): Broadcast[Map[Int, (Int, Int)]] = {
    spark.sparkContext.broadcast(policyData
      .join(seedPriority, Seq("SeedId"), "left")
      .select('SyntheticId, 'MappingId, coalesce('SeedPriority, lit(SeedPriority.Other.id)).as("SeedPriority"))
      .as[(Int, Int, Int)]
      .map(e => (e._1, (e._2, e._3)))
      .collect()
      .toMap)
  }

  def getSyntheticIdToMappingIdUdf(syntheticIdToMappingId: Map[Int, (Int, Int)]): UserDefinedFunction = {
    udf(
      (pairs: Seq[Int], maxLength: Int) => {
        val priorityCount = Array.fill(SeedPriority.values.size)(0)
        val result = pairs.flatMap(e => {
          val v = syntheticIdToMappingId.get(e)
          // count how many mapping ids in different priorities
          if (v.nonEmpty) priorityCount.update(v.get._2, priorityCount(v.get._2) + 1)
          v
        })
        if (pairs.length <= maxLength) {
          result.map(_._1)
        }
        else {
          var resultCount = 0
          var currentIndex = 0
          breakable {
            for (i <- priorityCount.indices) {
              resultCount = resultCount + priorityCount(i)
              if (resultCount >= maxLength) {
                currentIndex = i
                break
              }
            }
          }

          if (resultCount == maxLength) {
            result.filter(e => e._2 <= currentIndex).map(_._1)
          } else {
            val lowerPriorityResult = result.filter(e => e._2 < currentIndex).map(_._1)
            val currentPriorityResult = Random.shuffle(result.filter(e => e._2 == currentIndex).map(_._1)).take(maxLength - resultCount + priorityCount(currentIndex))
            lowerPriorityResult ++ currentPriorityResult
          }
        }
      }
    )
  }

  def runTransform(args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    runTransform(args)

    spark.catalog.clearCache()
    spark.stop()
  }

}

final case class DailySeedDensityScoreEntity( FeatureKey: String,
                                              FeatureValueHashed: Long,
                                              SeedId: String,
                                              DensityScore: Double )
