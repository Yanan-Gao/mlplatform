package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, sampleHit, trainSetDownSampleFactor}
import com.thetradedesk.audience.datasets.{AudienceExtensionSeedSettingsDataset, ExperimentEventDataset, ExperimentEventRecord, ExperimentHitDataset, ExperimentHitStage, ExperimentHitType, SeenInBiddingV3DeviceDataSet}
import com.thetradedesk.geronimo.shared.shiftModUdf
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object ExperimentMetricsReporter {
  def main(args: Array[String]): Unit = {
    val testHits = ExperimentHitDataset().readPartition(date, format=Some("tsv"))(spark)
      .filter('Stage === lit(ExperimentHitStage.Index.id))
    val modelHits = testHits.filter('Type === lit(ExperimentHitType.Model.id))
    val lalHits = testHits.filter('Type === lit(ExperimentHitType.LAL.id))

    val calibratedModelHits = calibrate(modelHits, lalHits)

    val experimentEvents = getExperimentEvents(date, calibratedModelHits, lalHits).cache

    ExperimentEventDataset().writePartition(
      experimentEvents,
      date,
      format = Some("parquet"),
      saveMode = SaveMode.Overwrite
    )
  }

  def calibrate(modelHits: Dataset[_], lalHits: Dataset[_]): Dataset[_] = {
    // Naive, to be replaced
    val window = Window.orderBy('AEScore)
    modelHits.withColumn("rowNumber", row_number().over(window))
      .filter('rowNumber <= lalHits.count)
      .drop("rowNumber")
  }

  def summarise(events: Dataset[_], groupBy: Seq[String] = Seq("Dummy")): Dataset[_] = {
    var taggedEvents = events
    if (!events.columns.contains("LookalikeCount")) {
      taggedEvents = taggedEvents
        .withColumn("LookalikeHit", ('LookalikeLabel && 'TrueLabel).cast("Int"))
        .withColumn("LookalikeCount", ('LookalikeLabel).cast("Int"))
        .withColumn("ModelHit", ('ModelLabel && 'TrueLabel).cast("Int"))
        .withColumn("ModelCount", ('ModelLabel).cast("Int"))
        .withColumn("HoldOutModelHit", ('WasHeldOut && 'ModelLabel && 'TrueLabel).cast("Int"))
        .withColumn("HoldOutModelCount", ('WasHeldOut && 'ModelLabel).cast("Int"))
        .withColumn("NonHoldOutModelHit", (!'WasHeldOut && 'ModelLabel && 'TrueLabel).cast("Int"))
        .withColumn("NonHoldOutModelCount", (!'WasHeldOut && 'ModelLabel).cast("Int"))
    }

    if (groupBy.length == 1 && groupBy(0) == "Dummy") {
      taggedEvents = taggedEvents.withColumn("Dummy", lit(1))
    }

    taggedEvents.groupBy(groupBy(0), groupBy.slice(1, groupBy.length): _*).agg(
      sum('LookalikeHit).as("LookalikeHit"),
      sum('LookalikeCount).as("LookalikeCount"),
      sum('ModelHit).as("ModelHit"),
      sum('ModelCount).as("ModelCount"),
      sum('HoldOutModelHit).as("HoldOutModelHit"),
      sum('HoldOutModelCount).as("HoldOutModelCount"),
      sum('NonHoldOutModelHit).as("NonHoldOutModelHit"),
      sum('NonHoldOutModelCount).as("NonHoldOutModelCount")
    )
      .withColumn("LookalikeHitRate", 'LookalikeHit / 'LookalikeCount)
      .withColumn("ModelHitRate", 'ModelHit / 'ModelCount)
      .withColumn("HoldOutModelHitRate", 'HoldOutModelHit / 'HoldOutModelCount)
      .withColumn("NonHoldOutModelHitRate", 'NonHoldOutModelHit / 'NonHoldOutModelCount)
      .drop("Dummy")
  }

  def getExperimentEvents(date: LocalDate, modelHits: Dataset[_], lalHits: Dataset[_]) = {

    val seedSettings = broadcast(AudienceExtensionSeedSettingsDataset().readLatestPartition()
      .withColumnRenamed("SeedId", "TargetingDataId"))

    val events = modelHits.select('AvailableBidRequestId, 'TDID, 'AdGroupId).join(seedSettings, Seq("AdGroupId")).drop("AdGroupId").withColumn("ModelLabel", lit(true))
      .join(lalHits.select('AvailableBidRequestId, 'TDID, 'AdGroupId).join(seedSettings, Seq("AdGroupId")).drop("AdGroupId").withColumn("LookalikeLabel", lit(true)), Seq("AvailableBidRequestId", "TDID", "TargetingDataId"), "outer")
      .withColumn("ModelLabel", when('ModelLabel.isNotNull, 'ModelLabel).otherwise(lit(false)))
      .withColumn("LookalikeLabel", when('LookalikeLabel.isNotNull, 'LookalikeLabel).otherwise(lit(false)))

    val relevantTargetingDataIds = spark.sparkContext.broadcast(events.select('TargetingDataId).distinct.as[Long].collect)

    val sib = SeenInBiddingV3DeviceDataSet().readPartition(date)(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .withColumn("FirstPartyTargetingDataIds", array_intersect('FirstPartyTargetingDataIds, typedLit(relevantTargetingDataIds.value)))
      .select('TDID, 'FirstPartyTargetingDataIds)

    val taggedEvents = events.join(sib, Seq("TDID"), "inner")
      .withColumn("TrueLabel", array_contains('FirstPartyTargetingDataIds, 'TargetingDataId))
      .withColumn("TargetingDataId", shiftModUdf('TargetingDataId, lit(2000003)))
      .withColumn("WasHeldOut", abs(hash(concat('TDID, 'TargetingDataId))) % trainSetDownSampleFactor =!= lit(sampleHit))
      .select('AvailableBidRequestId, 'TDID, 'TargetingDataId, 'LookalikeLabel, 'ModelLabel, 'TrueLabel, 'WasHeldOut)
      .as[ExperimentEventRecord]

    taggedEvents
  }
}