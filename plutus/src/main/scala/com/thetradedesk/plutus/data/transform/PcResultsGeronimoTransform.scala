package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema.{MinimumBidToWinData, PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.PlutusDataTransform.{loadPlutusLogData, loadRawMbtwData}
import com.thetradedesk.plutus.data.{IMPLICIT_DATA_SOURCE, loadParquetDataHourly, paddedDatePart, paddedDateTimePart}
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.spark.listener.WriteListener
import job.PcResultsGeronimoJob.{numRowsAbsent, numRowsWritten, ttdEnv}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDateTime

object PcResultsGeronimoTransform extends Logger {

  val spark = job.PcResultsGeronimoJob.spark
  import spark.implicits._
  def joinGeronimoPcResultsLog(
                                g: Dataset[BidsImpressionsSchema],
                                p: Dataset[PlutusLogsData],
                                m: Dataset[MinimumBidToWinData],
                                joinCols: Seq[String]
                              ): (Dataset[PcResultsMergedSchema], sql.DataFrame, sql.DataFrame) = {
    val mergedDataset = g.join(p, joinCols, "left").join(m, joinCols, "left").as[PcResultsMergedSchema]

    val pcResultsAbsentDataset = p.join(g, joinCols, "leftanti")
    val mbtwAbsentDataset = m.join(g, joinCols, "leftanti")

    (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset)
  }

  def transform(dateTime: LocalDateTime, fileCount: Int) = {
    val geronimoDataset = loadParquetDataHourly[BidsImpressionsSchema](
      f"${BidsImpressions.BIDSIMPRESSIONSS3}prod/bidsimpressions",
      dateTime,
      source = Some(IMPLICIT_DATA_SOURCE)
    )

    val pcResultsDataset = loadPlutusLogData(dateTime)
    val mbtwData = loadRawMbtwData(dateTime)

    // Possibly additional join columns: "AdgroupId", "SupplyVendor"
    val joinCols = Seq("BidRequestId")
    val (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset) = joinGeronimoPcResultsLog(geronimoDataset, pcResultsDataset, mbtwData, joinCols)
    val pcResultsAbsentCount = pcResultsAbsentDataset.count()
    val mbtwAbsentCount = mbtwAbsentDataset.count()

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    val outputPath = f"${PcResultsMergedDataset.S3PATH}${PcResultsMergedDataset.S3PATH_GEN.apply(dateTime)}"
    mergedDataset.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)

    val rows = listener.rowsWritten
    println(s"Rows Written: $rows")

    numRowsWritten.set(rows)
    numRowsAbsent.labels("pcResultsLog").set(pcResultsAbsentCount)
    numRowsAbsent.labels("mbtw").set(mbtwAbsentCount)
  }
}
