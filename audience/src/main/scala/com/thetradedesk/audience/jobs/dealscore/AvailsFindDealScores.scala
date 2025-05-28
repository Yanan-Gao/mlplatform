package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.isDeviceIdSampled1Percent
//import com.thetradedesk.availspipeline.spark.datasets.{DealAvailsAggDailyDataSet, IdentityAndDealAggHourlyDataSet}
import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AvailsFindDealScores {
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val br_emb_path= config.getString(
    "br_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/deal_score/emb/raw/v=1/date=${dateStr}/")
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  //val avails_path = config.getString(
  //  "avails_path", s"s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-deal-agg-hourly-delta/date=${dateStrDash}")
  val users_scores_path = config.getString("users_scores_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid/v=1/date=${dateStr}/")
  val seed_id_path = config.getString(
    "seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/")

  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score/v=1/date=${dateStr}/")
  val agg_score_out_path = config.getString("agg_score_out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_agg/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )

  val cap_users = config.getInt("cap_users", 15000)
  val min_users = config.getInt("min_users", 3000)

  case class UserIdentifier(identitySource: Int, guidString: String)

  // Define the function to coalesce all the ID to get final TDID
  //TODO: create mapping for https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Shared/EnumsAndConstants/TTD.Domain.Shared.EnumsAndConstants.Identity/IdentitySource.cs
  //public enum IdentitySource
  //    {
  //        Unknown = 0,
  //        Tdid = 1,
  //        MiscDeviceId = 2,
  //        UnifiedId2 = 3, // when consuming this value, ensure there is equivalent handling for IdentitySource.EUID (or add a comment if explicitly excluding EUID)
  //        IdentityLinkId = 4,
  //        EUID = 5, // when consuming this value, ensure there is equivalent handling for IdentitySource.UnifiedId2 (or add a comment if explicitly excluding UnifiedId2)
  //        Nebula = 6,
  //        HashedIP = 7,
  //        CrossDevice = 8, // !!Important!! This value is deliberately not in dbo.IdentitySource in the database.
  //        // This is a fake value used when updating the adfrequency source used (see the function GetLoggedBitmapFromIdentitySources for example).
  //        DATId = 9,
  private def processUserIdentifiers(userIdentifiers: Seq[UserIdentifier]): String = {
    val priorityOrder = Seq(1, 2, 3, 4, 5)
    userIdentifiers
      .filter(ui => priorityOrder.contains(ui.identitySource))
      .sortBy(ui => priorityOrder.indexOf(ui.identitySource))
      .headOption
      .map(_.guidString)
      .getOrElse("")
  }
  private val processUserIdentifiersUDF: UserDefinedFunction = udf(processUserIdentifiers _)

  def runETLPipeline(): Unit = {

    /*
    val dealSv = spark.read.parquet(deal_sv_path)
    val dealSvSeedDS = dealSv.select("DealCode", "SupplyVendorId", "SeedId").distinct().cache();
    val dealSvDS = dealSvSeedDS.select("DealCode", "SupplyVendorId").distinct()

    val dayToRead = LocalDate.parse(dateStrDash).atStartOfDay()
    val avails = IdentityAndDealAggHourlyDataSet.readFullDayPartition(dayToRead)

    val availsFiltered = avails.filter(
        size(col("DealCodes")) > 0 && !col("DealCodes")(0).isNull && col("DealCodes")(0) =!= "" &&
        expr("exists(UserIdentifiers, x -> x.identitySource IN (1,2,3,4,5))")
    ).select("UserIdentifiers", "DealCodes", "SupplyVendorId")

    val availsProcessed = availsFiltered.withColumn("TDID", processUserIdentifiersUDF(col("UserIdentifiers"))).select("TDID", "DealCodes", "SupplyVendorId")

    val availsSampled = availsProcessed
      .filter(isDeviceIdSampled1Percent('TDID))
      .filter("SUBSTRING(TDID, 9, 1) = '-'") // Assure TDID is in uuid format, same logic is applied in offline user scores job
      .select('TDID,
        'DealCodes,
        'SupplyVendorId)

    val explodedAvailsProcessed = availsSampled.withColumn("DealCode", explode(col("DealCodes"))).select('TDID, 'DealCode, 'SupplyVendorId)

    //TODO verify the need for trim
    val matched_df = explodedAvailsProcessed.as("a").join(
      broadcast(dealSvDS.as("d")),
      trim(col("a.DealCode")) === trim(col("d.DealCode")) && col("a.SupplyVendorId") === col("d.SupplyVendorId")
    )

    //TODO might need to count by TDID to do a weighted average
    val TdidsByDCSV =  matched_df.select("TDID", "a.DealCode", "a.SupplyVendorId").distinct()

    val user_scores = spark.read.format("parquet").load(users_scores_path)
    val seedIds = spark.read.format("parquet").load(seed_id_path)

    val posexplodedSeedsWithRowNumber = seedIds.selectExpr("posexplode(SeedId) as (pos, SeedId)")

    //formula is 4 * cpu * number of machines
    val numberOfPartitions = num_workers * 4 * cpu_per_worker
    val TdidsWithScore = TdidsByDCSV.repartition(numberOfPartitions, col("TDID")).join(user_scores.repartition(numberOfPartitions, col("TDID")), Seq("TDID")).select(TdidsByDCSV("TDID"), TdidsByDCSV("DealCode"), TdidsByDCSV("SupplyVendorId"), user_scores("Score"))
    val TdisWithScoreWithSdf = TdidsWithScore.join(broadcast(dealSvSeedDS), trim(TdidsWithScore("DealCode")) === trim(dealSvSeedDS("DealCode")) && TdidsWithScore("SupplyVendorId") === dealSvSeedDS("SupplyVendorId"), "inner").select(col("TDID"), TdidsWithScore("DealCode"), TdidsWithScore("SupplyVendorId"), col("Score"), col("SeedId"))

    val finalDf = TdisWithScoreWithSdf.join(broadcast(posexplodedSeedsWithRowNumber), Seq("SeedId")).withColumn("Score", element_at(col("Score"), col("pos") + 1)).select(TdisWithScoreWithSdf("TDID"), TdisWithScoreWithSdf("DealCode"), TdisWithScoreWithSdf("SupplyVendorId"),  TdisWithScoreWithSdf("SeedId"), col("Score"))

    val structuredFinal = finalDf.withColumn("tdidValueStruct", struct(col("TDID"), col("Score")))

    val windowSpec = Window.partitionBy("DealCode", "SupplyVendorId", "SeedId").orderBy(col("rand"))
    val structuredFinalWithRowNum = structuredFinal
      .repartition(numberOfPartitions, col("DealCode"), col("SupplyVendorId"), col("SeedId"))
      .withColumn("rand", rand())
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") <= cap_users)
      .drop("row_num")

    val raw_out = structuredFinalWithRowNum
      .groupBy("DealCode", "SupplyVendorId", "SeedId")
      .agg(collect_list(col("tdidValueStruct")).as("Scores"))
      //.withColumn("Scores", expr(s"slice(Scores, 1, $cap_users)"))

    raw_out
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(out_path)

    val raw_scores = spark.read.format("parquet").load(out_path)
    raw_scores.withColumn("Score", explode($"Scores.Score"))
      .groupBy("DealCode", "SupplyVendorId", "SeedId")
      .agg(
        count("Score").as("UserCount"),
        avg("Score").as("AverageScore")
      )
      .filter(col("UserCount") >= min_users)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(agg_score_out_path)
     */

  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}

