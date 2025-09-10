package com.thetradedesk.audience.jobs.modelinput.relevanceboostmodel

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, RelevanceBoostModelInputDataset, RelevanceBoostModelInputRecord}
import com.thetradedesk.audience._
import org.apache.spark.sql._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object RelevanceBoostModelInputGeneratorJob {

  def getDateStr(date: LocalDate): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    date.format(dtf)
  }

  def transform(
                 policyTableDf: DataFrame,
                 oosPredictionDf: DataFrame,
                 geronimoDf: DataFrame,
                 lalResultsDf: DataFrame
               ): Dataset[RelevanceBoostModelInputRecord] = {

    val policyTable = policyTableDf
      .where('CrossDeviceVendorId === 0 && 'Source === 3)

    val oosPrediction = oosPredictionDf
      .select(
        $"BidRequestId",
        $"Targets"(0).cast(FloatType).as("Target"),
        $"pred"(0).cast(FloatType).as("pred"),
        $"sen_pred"(0).cast(FloatType).as("sen_pred"),
        $"SyntheticIds"(0).as("SyntheticId"),
        $"ZipSiteLevel_Seed"(0).cast(IntegerType).as("ZipSiteLevel_Seed")
      )
      .join(policyTable, Seq("SyntheticId"), "inner")
      .withColumn("Original_NeoScore", when($"IsSensitive", $"sen_pred").otherwise($"pred"))
      .filter($"Original_NeoScore".isNotNull)
      .select($"BidRequestId", $"Target", $"IsSensitive", $"Original_NeoScore", $"SourceId".as("SeedId"), $"ZipSiteLevel_Seed")

    val geronimo = geronimoDf
      .filter($"IsImp"===1)
      .select($"BidRequestId", $"AdvertiserId", $"CampaignId", $"AdGroupId", $"UserTargetingDataIds".as("TargetingDataIds"), $"AdvertiserFirstPartyDataIds")

    val oos = geronimo
      .join(oosPrediction, Seq("BidRequestId"), "inner")

    val oosTargetingDataExploded = oos
      .select($"BidRequestId", $"SeedId", explode($"TargetingDataIds").as("TargetingDataId"))

    val oosAdvertiserFirstPartyDataExploded = oos
      .select($"BidRequestId", $"SeedId", explode($"AdvertiserFirstPartyDataIds").as("TargetingDataId"))

    val lalResults = lalResultsDf
      .select($"TargetingDataId", $"RelevanceRatio", $"SeedId", $"IsFirstPartyData")

    val mergeTargetingDataRelevanceScores = udaf(TargetingDataRelevanceScoreAggregator)

    val oosAdvertiserFirstPartyDataMap = oosAdvertiserFirstPartyDataExploded
      .join(lalResults, Seq("SeedId", "TargetingDataId"), "left")
      .withColumn("RelevanceRatio", when($"RelevanceRatio".isNotNull, $"RelevanceRatio").otherwise(lit(0.0)))
      .groupBy("BidRequestId", "SeedId")
      .agg(
        mergeTargetingDataRelevanceScores($"TargetingDataId", $"RelevanceRatio").as("AdvertiserFirstPartyDataRelevanceMap")
      )

    oosTargetingDataExploded
      .join(lalResults, Seq("SeedId", "TargetingDataId"), "left")
      .withColumn("RelevanceRatio", when($"RelevanceRatio".isNotNull, $"RelevanceRatio").otherwise(lit(0.0)))
      .groupBy("BidRequestId", "SeedId")
      // aggregate statistics
      .agg(
        mergeTargetingDataRelevanceScores($"TargetingDataId", $"RelevanceRatio").as("DataRelevanceMap"),

        // third party data features
        percentile_approx(when($"IsFirstPartyData" === false, $"RelevanceRatio"), lit(0.5), lit(10000)).cast(FloatType).as("MatchedTpdRelevanceP50"),
        percentile_approx(when($"IsFirstPartyData" === false, $"RelevanceRatio"), lit(0.9), lit(10000)).cast(FloatType).as("MatchedTpdRelevanceP90"),
        sum(when($"IsFirstPartyData" === false, $"RelevanceRatio")).cast(FloatType).as("MatchedTpdRelevanceSum"),
        stddev(when($"IsFirstPartyData" === false, $"RelevanceRatio")).cast(FloatType).as("MatchedTpdRelevanceStddev"),
        count(when($"IsFirstPartyData" === false, $"TargetingDataId")).cast(IntegerType).as("MatchedTpdCount"),

        // first party data features
        percentile_approx(when($"IsFirstPartyData" === true, $"RelevanceRatio"), lit(0.5), lit(10000)).cast(FloatType).as("MatchedFpdRelevanceP50"),
        percentile_approx(when($"IsFirstPartyData" === true, $"RelevanceRatio"), lit(0.9), lit(10000)).cast(FloatType).as("MatchedFpdRelevanceP90"),
        sum(when($"IsFirstPartyData" === true, $"RelevanceRatio")).cast(FloatType).as("MatchedFpdRelevanceSum"),
        stddev(when($"IsFirstPartyData" === true, $"RelevanceRatio")).cast(FloatType).as("MatchedFpdRelevanceStddev"),
        count(when($"IsFirstPartyData" === true, $"TargetingDataId")).cast(IntegerType).as("MatchedFpdCount")
      )
      .join(oosAdvertiserFirstPartyDataMap, Seq("BidRequestId", "SeedId"), "left")
      .join(oos, Seq("BidRequestId", "SeedId")) // join aggregation result back with oos
      // multiplication features
      .withColumn("MatchedFpdRelevanceP90xZipSiteLevel", $"MatchedFpdRelevanceP90" * $"ZipSiteLevel_Seed")
      .withColumn("MatchedTpdRelevanceP90xZipSiteLevel", $"MatchedTpdRelevanceP90" * $"ZipSiteLevel_Seed")
      .withColumn("MatchedFpdRelevanceP90xOriginal_NeoScore", $"MatchedFpdRelevanceP90" * $"Original_NeoScore")
      .withColumn("MatchedTpdRelevanceP90xOriginal_NeoScore", $"MatchedTpdRelevanceP90" * $"Original_NeoScore")
      .selectAs[RelevanceBoostModelInputRecord]
  }

  def readLalResults(date: LocalDate): DataFrame = {
    val lalReadEnv = config.getString("lalReadEnv", ttdReadEnv)

    // TODO: route it to the final segment density dataset when it's ready
    val lalDateStr = getDateStr(date.minusDays(2))

    val thirdParty = spark.read.parquet(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/all_model_unfiltered_results/v=1/idCap=100000/date=$lalDateStr")
      .select($"SeedId", $"TargetingDataId", $"RelevanceRatio", lit(false).as("IsFirstPartyData"))

    val firstParty = spark.read.parquet(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/1p_all_model_unfiltered_results/v=1/idCap=100000/date=$lalDateStr")
      .select($"SeedId", $"TargetingDataId", $"RelevanceRatio", lit(true).as("IsFirstPartyData"))

    thirdParty.union(firstParty)
  }

  def readOOSPrediction(date: LocalDate): DataFrame = {
    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/data/${config.getString("oosPredictionReadEnv", ttdEnv)}/audience/RSMV2/prediction/oos_data/v=1/model_version=${getDateStr(date.minusDays(1))}000000/${getDateStr(date)}000000/")
  }

  def readGeronimo(date: LocalDate): DataFrame = {
    val dateStr = getDateStr(date)
    val yyyy = dateStr.substring(0, 4)
    val mm = dateStr.substring(4, 6)
    val dd = dateStr.substring(6, 8)

    spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/")
  }

  def main(args: Array[String]): Unit = {
    val policyTableDf = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark)
      .select($"CrossDeviceVendorId", $"Source", $"SourceId", $"SyntheticId", $"IsSensitive")

    val oosPredictionDf = readOOSPrediction(date)

    val geronimoDf = readGeronimo(date)

    val lalDf = readLalResults(date)

    val modelInput = transform(policyTableDf.toDF(), oosPredictionDf, geronimoDf, lalDf)

    RelevanceBoostModelInputDataset("Seed_None").writePartition(
      modelInput,
      dateTime,
      saveMode = SaveMode.Overwrite
    )
  }
}

object TargetingDataRelevanceScoreAggregator extends Aggregator[(Long, Double), mutable.Map[Long, Double], Map[Long, Double]] with Serializable {
  override def zero: mutable.Map[Long, Double] = mutable.Map.empty

  override def reduce(buffer: mutable.Map[Long, Double], input: (Long, Double)): mutable.Map[Long, Double] = {
    if (input != null) {
      buffer.update(input._1, input._2)
    }
    buffer
  }

  override def merge(b1: mutable.Map[Long, Double], b2: mutable.Map[Long, Double]): mutable.Map[Long, Double] = {
    b2.foreach { case (k, v) => b1.update(k, v) }
    b1
  }

  override def finish(reduction: mutable.Map[Long, Double]): Map[Long, Double] = reduction.toMap

  override def bufferEncoder: Encoder[mutable.Map[Long, Double]] = Encoders.kryo[mutable.Map[Long, Double]]
  override def outputEncoder: Encoder[Map[Long, Double]] = ExpressionEncoder.apply[Map[Long, Double]]
}
