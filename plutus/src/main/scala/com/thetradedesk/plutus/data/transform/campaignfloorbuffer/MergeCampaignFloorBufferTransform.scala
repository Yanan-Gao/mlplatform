package com.thetradedesk.plutus.data.transform.campaignfloorbuffer

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.{CampaignFloorBufferDataset, CampaignFloorBufferSchema, MergedCampaignFloorBufferDataset, MergedCampaignFloorBufferSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.CampaignFlightData
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import com.thetradedesk.spark.TTDSparkContext.spark
import job.campaignbackoff.CampaignAdjustmentsJob.date
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionJob.numAdhocFloorBufferRowsWritten
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{coalesce, col, lit, when}

import java.time.LocalDate

object MergeCampaignFloorBufferTransform {

  val experimentFloorBufferTag = "Experiment"
  val automatedFloorBufferTag = "AutomatedOnePercentSelection"

  def readMergedFloorBufferData(): Dataset[MergedCampaignFloorBufferSchema] = {
    val mergedBufferFloorData = try {
      MergedCampaignFloorBufferDataset.readDate(date, envForReadInternal)
        .distinct()
    } catch {
      case _: S3NoFilesFoundException => Seq.empty[MergedCampaignFloorBufferSchema].toDS()
    }
    mergedBufferFloorData
  }

  def mergeFloorBufferAndAdhocData(todaysCampaignFloorBuffer: Dataset[CampaignFloorBufferSchema],
                                   todaysAdhocCampaignFloorBuffer: Dataset[CampaignFloorBufferSchema]): Dataset[MergedCampaignFloorBufferSchema] = {
    val adhocFloorBufferColRenamed = todaysAdhocCampaignFloorBuffer
      .withColumnRenamed("BBF_FloorBuffer", "AdhocBBF_FloorBuffer")
      .withColumnRenamed("AddedDate", "AdhocBBF_AddedDate")

    // Merge today's campaign floor buffer data and adhoc data for experiment.
    // If a campaign found in adhoc data also belongs to another criteria, do not override it.
    // Assign Floor buffer as Automated to the floor buffer set through CampaignFloorBufferCandidateSelectionTransform
    // and Adhoc to the campaigns used for experiment.
    todaysCampaignFloorBuffer.join(adhocFloorBufferColRenamed, Seq("CampaignId"), "fullouter")
      .withColumn("AddedDate", coalesce(col("AdhocBBF_AddedDate"), col("AddedDate")))
      .withColumn("BufferType", when(col("BBF_FloorBuffer").isNotNull, lit(automatedFloorBufferTag)).otherwise(lit(experimentFloorBufferTag)))
      .withColumn("BBF_FloorBuffer", coalesce(col("BBF_FloorBuffer"), col("AdhocBBF_FloorBuffer")))
      .selectAs[MergedCampaignFloorBufferSchema]
      .distinct()
  }

  def getTodaysAdhocFloorBufferData(date: LocalDate,
                                    expFloorBuffer: Double,
                                    liveExperimentCampaigns: Dataset[CampaignFlightData]): Dataset[CampaignFloorBufferSchema] = {
    liveExperimentCampaigns.withColumn("BBF_FloorBuffer", lit(expFloorBuffer))
      .withColumn("AddedDate", lit(date))
      .selectAs[CampaignFloorBufferSchema]
      .distinct()
  }

  def transform(date: LocalDate, fileCount: Int,
                expFloorBuffer: Double,
                todaysCampaignFloorBufferData: Dataset[CampaignFloorBufferSchema],
                liveExperimentCampaigns: Dataset[CampaignFlightData]) = {
    val todaysAdhocFloorBufferData = getTodaysAdhocFloorBufferData(
      date=date,
      expFloorBuffer=expFloorBuffer,
      liveExperimentCampaigns=liveExperimentCampaigns)
    numAdhocFloorBufferRowsWritten.set(todaysAdhocFloorBufferData.count())

    val mergedCampaignFloorBufferData = mergeFloorBufferAndAdhocData(
      todaysCampaignFloorBuffer=todaysCampaignFloorBufferData,
      todaysAdhocCampaignFloorBuffer=todaysAdhocFloorBufferData)

    // Write the merged buffer floor snapshot to S3
    MergedCampaignFloorBufferDataset.writeData(
      date=date,
      dataset=mergedCampaignFloorBufferData,
      filecount=fileCount)
  }
}
