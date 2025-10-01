package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.deals.utils.OpenMarketDealUtils
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter

object AvailsFindDealsOpenMarket {
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val deals_OM_equivalents_path  = config.getString("deals_OM_equivalents_path", s"s3://ttd-deal-quality/env=prod/DealQualityMetrics/DealOpenMarketEquivalents/v=1/date=${dateStr}/" )
  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_tdids/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )
  val findDealsCoalesce = config.getInt("findDealsCoalesce", 4096)

  val samplingRate = config.getInt("sampling_rate", 3)
  val min_hour = config.getInt("min_hour", 0)
  val max_hour = config.getInt("max_hour", 24)
  val limit_users = config.getInt("limit_users", 50000)
  val min_adInfoCount = config.getInt("min_adInfoCount", 100000)

  private def explodeBitmask(bitmask: Int): Seq[Int] = {
    (0 until 8).filter(i => (bitmask & (1 << i)) > 0)
  }

  val explodeInventoryChannelBitmaskUDF = udf((bitmask: Int) => explodeBitmask(bitmask))
  private def hasSp500MatchFunc(supplyVendorId: Long,
                                dealCodes: Seq[String],
                                supplyVendorIdToCodes: Map[Long, Set[String]] ): Boolean = {
    supplyVendorIdToCodes.get(supplyVendorId) match {
      case Some(validCodes) => dealCodes.exists(validCodes.contains)
      case None => false
    }
  }

  def makeMatchUdf(supplyVendorIdToCodes: Broadcast[Map[Long, Set[String]]]) : UserDefinedFunction = {
    udf((supplyVendorId: Long, dealCodes: Seq[String]) => {
      hasSp500MatchFunc(
        supplyVendorId,
        dealCodes,
        supplyVendorIdToCodes.value
      )
    })
  }

  def runETLPipeline(): Unit = {
    val dealSv = spark.read.parquet(deal_sv_path)

    //hardcode for now
    val deals_om_equivalent = spark.read.parquet(deals_OM_equivalents_path)
      .filter(col("AdInfoCounts") > min_adInfoCount)
      .withColumnRenamed("PropertyId", "PropertyIdString")
    val deal_open_market = deals_om_equivalent.select(col("SupplyVendorId"), col("DealCode"), col("InventoryChannel"), col("PropertyIdString"), col("Country")).distinct().join(dealSv, Seq("SupplyVendorId", "DealCode"))

    val props_om_seed = deal_open_market.select(col("InventoryChannel"), col("PropertyIdString"), col("Country"), col("SeedId")).distinct().cache()
    val pc = props_om_seed.select(col("InventoryChannel"), col("PropertyIdString"), col("Country")).distinct().cache()

    val avails = DealScoreUtils.loadAvailsData(dateStrDash, min_hour, max_hour)

    val spd = OpenMarketDealUtils.getOpenMarketDeals(date, excludeNonRonDeals = false, excludeArchived = false).collect()
    val supplyVendorIdToCodes: Broadcast[Map[Long, Set[String]]] = spark.sparkContext.broadcast(spd.map { row =>
      row.SupplyVendorId.asInstanceOf[Long] -> row.openMarketDealCodes.toSet.asInstanceOf[Set[String]]
    }.toMap)
    val hasSp500Match = makeMatchUdf(supplyVendorIdToCodes)

    val availsProcessed = DealScoreUtils.processUserIdentifiers(avails, samplingRate,
      Seq("UserIdentifiers", "DealCodes", "allMediaTypesRequirePrivateContractMatch", "SupplyVendorId",
        "inventoryChannels", "PropertyIdString", "Country"))

    val availsOpenMarket = availsProcessed.filter(hasSp500Match(col("SupplyVendorId"), col("DealCodes"))
      || not(col("allMediaTypesRequirePrivateContractMatch"))
    )

    val availsWithInventoryChannels = availsOpenMarket
      .withColumn("InventoryChannels", explodeInventoryChannelBitmaskUDF(col("InventoryChannels")))

    val TDIDByProps = DealScoreUtils.explodeJoinAndDistinct(
      sourceDF = availsWithInventoryChannels,
      explodeColumns = Map("Identifiers" -> "TDID", "InventoryChannels" -> "InventoryChannel"),
      selectAfterExplode = Seq("TDID", "InventoryChannel", "PropertyIdString", "Country"),
      joinDF = pc,
      joinColumns = Seq("InventoryChannel", "PropertyIdString", "Country")
    )

    val numberOfPartitions = num_workers * 4 * cpu_per_worker
    DealScoreUtils.limitAndSave(
      sourceDF = TDIDByProps,
      partitionColumns = Seq("InventoryChannel", "PropertyIdString", "Country"),
      numberOfPartitions = numberOfPartitions,
      limitUsers = limit_users,
      coalesceTo = findDealsCoalesce,
      outPath = out_path
    )
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
