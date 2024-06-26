package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.defaultCloudProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDate

/**
 * This class is used to generate model training samples for audience extension model
 * using conversion tracker dataset
 */
case class AEMConversionInputGenerator(override val sampleRate: Double) extends AudienceModelInputGenerator("AEMConversion", sampleRate) {
  private val mappingSchema = StructType(
    Array(
      StructField("TrackingTagId", StringType, nullable = false),
      StructField("SyntheticId", IntegerType, nullable = false)
    ))


  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val mappingRows = policyTable.
      map(e => Row(e.SourceId, e.SyntheticId))
      .toSeq

    val mappingDataset = broadcast(
      spark.createDataFrame(
        spark.sparkContext.parallelize(mappingRows),
        mappingSchema))

    ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(AudienceModelInputGeneratorConfig.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(samplingFunction('TDID))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)
      .join(mappingDataset, "TrackingTagId")
      .groupBy('TDID)
      .agg(collect_set('SyntheticId) as "PositiveSyntheticIds")
      .withColumn("GroupId", 'TDID)
      .cache()
  }
}

