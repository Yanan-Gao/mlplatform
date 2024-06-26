package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDate

/**
 * This class is used to generate model training samples for first party pixel model
 * using seenInBidding dataset
 *
 */
case class AEMSIBInputGenerator(override val sampleRate: Double) extends AudienceModelInputGenerator("AEMSIB", sampleRate) {

  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]): DataFrame = {
    val mappingFunction = mappingFunctionGenerator(
      policyTable.map(e => (e.TargetingDataId, e.SyntheticId)).toMap)

    // FIXME use cross device SIB dataset to replace this one
    SeenInBiddingV3DeviceDataSet().readPartition(date, lookBack = Some(AudienceModelInputGeneratorConfig.seenInBiddingLookBack))(spark)
      .withColumnRenamed("DeviceId", "TDID")
      .filter(samplingFunction('TDID))
      // only support first party targeting data ids in current solution
      .withColumn("PositiveSyntheticIds", mappingFunction('FirstPartyTargetingDataIds))
      .filter(size('PositiveSyntheticIds) > AudienceModelInputGeneratorConfig.minimalPositiveLabelSizeOfSIB)
      .select('TDID, 'TDID.alias("GroupId"), 'PositiveSyntheticIds)
      .cache()
  }
}
