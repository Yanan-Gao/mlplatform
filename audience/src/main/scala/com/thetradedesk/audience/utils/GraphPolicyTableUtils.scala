package com.thetradedesk.audience.utils

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate

object GraphPolicyTableUtils {

  /**
    * Count raw seeds grouped by source id and original flag.
    */
  def rawSeedCount(allFinalSeedData: DataFrame): DataFrame = {
    allFinalSeedData
      .select(explode(col("SourceIds")).as("SourceId"), col("IsOriginal"))
      .groupBy("SourceId", "IsOriginal")
      .count()
      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
  }

  /**
    * Count seeds extended through the given graph type and merge with raw counts.
    */
  def extendGraphSeedCount(nonGraphCount: DataFrame,
                           allFinalSeedData: DataFrame,
                           crossDeviceVendor: CrossDeviceVendor): DataFrame = {
    val column = if (crossDeviceVendor == CrossDeviceVendor.IAV2Person) {
      "PersonGraphSeedIds"
    } else {
      "HouseholdGraphSeedIds"
    }

    allFinalSeedData
      .select(explode(col(column)).as("SourceId"), col("IsOriginal"))
      .groupBy("SourceId", "IsOriginal")
      .count()
      .join(nonGraphCount
              .select(col("SourceId"), col("IsOriginal"), col("count").as("RawCount")),
            Seq("SourceId", "IsOriginal"),
            "left")
      .withColumn("RawCount", coalesce(col("RawCount"), lit(0)))
      .select(col("SourceId"), col("IsOriginal"), (col("count") + col("RawCount")).as("count"))
      .withColumn("CrossDeviceVendorId", lit(crossDeviceVendor.id))
  }

  /**
    * Assemble the final policy table given source metadata and aggregated seed data.
    */
  def generateRawPolicyTable(sourceMeta: Dataset[_],
                             finalSeedData: DataFrame,
                             userDownSampleBasePopulation: Long,
                             userDownSampleHitPopulation: Long,
                             goalType: Int,
                             storageCloud: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val nonGraphCount = rawSeedCount(finalSeedData)
    val personGraphCount = extendGraphSeedCount(nonGraphCount, finalSeedData, CrossDeviceVendor.IAV2Person)
    val householdGraphCount = extendGraphSeedCount(nonGraphCount, finalSeedData, CrossDeviceVendor.IAV2Household)

    nonGraphCount
      .union(personGraphCount)
      .union(householdGraphCount)
      .groupBy("SourceId", "CrossDeviceVendorId")
      .agg(
        sum(when(col("IsOriginal") === 1, col("count")).otherwise(lit(0))).as("ActiveSize"),
        sum(col("count")).as("ExtendedActiveSize")
      )
      .join(sourceMeta, Seq("SourceId"), "inner")
      .select(
        (col("ActiveSize") * (userDownSampleBasePopulation.toDouble / userDownSampleHitPopulation.toDouble)).as("ActiveSize"),
        (col("ExtendedActiveSize") * (userDownSampleBasePopulation.toDouble / userDownSampleHitPopulation.toDouble)).as("ExtendedActiveSize"),
        col("CrossDeviceVendorId"),
        col("SourceId"),
        col("Count").as("Size"),
        col("TargetingDataId"),
        col("Source"),
        col("topCountryByDensity"),
        col("PermissionTag"),
        lit(goalType).as("GoalType"),
        lit(storageCloud).as("StorageCloud")
      )
  }
}

