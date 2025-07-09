package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.{_isDeviceIdSampled1Percent, _isDeviceIdSampledNPercent}
import com.thetradedesk.availspipeline.spark.datasets.{DealAvailsAggDailyDataSet, IdentityAndDealAggHourlyDataSet}
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
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_tdids/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )

  val samplingRate = config.getInt("sampling_rate", 3)
  val min_hour = config.getInt("min_hour", 0)
  val max_hour = config.getInt("max_hour", 24)
  val limit_users = config.getInt("limit_users", 50000)

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
  private def processUserIdentifiers(userIdentifiers: Seq[UserIdentifier]): Seq[String] = {
    val priorityOrder = Seq(1, 2, 3, 4, 5)
    userIdentifiers
      .filter(ui => priorityOrder.contains(ui.identitySource))
      .filter(ui => _isDeviceIdSampledNPercent(ui.guidString, samplingRate))
      .filter(ui => ui.guidString.length > 8 && ui.guidString.charAt(8) == '-')
      .map(_.guidString)
  }
  private val processUserIdentifiersUDF: UserDefinedFunction = udf(processUserIdentifiers _)

  def runETLPipeline(): Unit = {

    val dealSv = spark.read.parquet(deal_sv_path)
    val dealSvSeedDS = dealSv.select("DealCode", "SupplyVendorId", "SeedId").distinct().cache();
    val dealSvDS = dealSvSeedDS.select("DealCode", "SupplyVendorId").distinct()

    val dayToRead = LocalDate.parse(dateStrDash).atStartOfDay()
    val avails = IdentityAndDealAggHourlyDataSet.readFullDayPartition(dayToRead).filter(col("hour") >= min_hour && col("hour") < max_hour)

    val availsFiltered = avails.filter(
        size(col("DealCodes")) > 0 && !col("DealCodes")(0).isNull && col("DealCodes")(0) =!= "" &&
        expr("exists(UserIdentifiers, x -> x.identitySource IN (1,2,3,4,5))")
    ).select("UserIdentifiers", "DealCodes", "SupplyVendorId")

    val availsProcessed = availsFiltered.withColumn("Identifiers", processUserIdentifiersUDF(col("UserIdentifiers")))
      .filter(size(col("Identifiers")) > 0)
      .select("Identifiers", "DealCodes", "SupplyVendorId")

    val availsProcessedExploded = availsProcessed.withColumn("TDID", explode(col("Identifiers"))).withColumn("DealCode", explode(col("DealCodes"))).select('TDID, 'DealCode, 'SupplyVendorId)

    val matched_df = availsProcessedExploded.as("a").join(
      broadcast(dealSvDS.as("d")),
      trim(col("a.DealCode")) === trim(col("d.DealCode")) && col("a.SupplyVendorId") === col("d.SupplyVendorId")
    ).select("TDID", "a.DealCode", "a.SupplyVendorId")

    val TdidsByDCSV = matched_df.select("TDID", "DealCode", "SupplyVendorId").distinct()

    val numberOfPartitions = num_workers * 4 * cpu_per_worker
    val windowSpec = Window.partitionBy("DealCode", "SupplyVendorId").orderBy(col("rand"))

    TdidsByDCSV
      .repartition(numberOfPartitions, col("DealCode"), col("SupplyVendorId"))
      .withColumn("rand", rand())
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") <= limit_users)
      .drop("row_num")
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(out_path)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
