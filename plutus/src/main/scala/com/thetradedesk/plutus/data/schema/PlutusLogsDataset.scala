package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.{loadParquetDataHourlyV2, paddedDatePart}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset

import java.time.LocalDateTime

case class PlutusLogsData(
                           BidRequestId: String,
                           //  Not using these two fields since we're getting them from
                           //  Geronimo and having them makes joining more complicated.
                           //    SupplyVendor: String,
                           //    AdgroupId: String,

                           InitialBid: Double,
                           FinalBidPrice: Double,
                           Discrepancy: Double,
                           BaseBidAutoOpt: Double,
                           LegacyPcPushdown: Double,

                           OptOutDueToFloor: Boolean,
                           FloorPrice: Double,
                           PartnerSample: Boolean,
                           BidBelowFloorExceptedSource: Int,
                           FullPush: Boolean,

                           // Fields From PlutusLog
                           Mu: Float,
                           Sigma: Float,
                           GSS: Double,
                           AlternativeStrategyPush: Double,

                           // Fields from PredictiveClearingStrategy
                           Model: String,
                           Strategy: Int,
                         )

case object PlutusLogsData {
  def transformPcResultsRawLog(value: Dataset[PcResultsRawLogs]): Dataset[PlutusLogsData] = {
    value.select(
      "PlutusLog.*",
      "PredictiveClearingStrategy.*",
      "*"
    ).drop(
      "PlutusLog",
      "PredictiveClearingStrategy"
    ).as[PlutusLogsData]
  }

  def loadPlutusLogData(dateTime: LocalDateTime): Dataset[PlutusLogsData] = {
    val dataset = loadParquetDataHourlyV2[PcResultsRawLogs](
      PlutusLogsDataset.S3PATH,
      PlutusLogsDataset.S3PATH_GEN,
      dateTime
    )
    transformPcResultsRawLog(dataset)
  }
}


/**
 * This class is used to read the raw pcresults dataset from s3. On reading, its immediately transformed
 * into @PlutusLogsData
 */

case class PcResultsRawLogs(
                             BidRequestId: String,
                             InitialBid: Double,
                             FinalBidPrice: Double,
                             Discrepancy: Double,
                             BaseBidAutoOpt: Double,
                             LegacyPcPushdown: Double,
                             PlutusLog: PlutusLog,
                             PredictiveClearingStrategy: PredictiveClearingStrategy,
                             OptOutDueToFloor: Boolean,
                             FloorPrice: Double,
                             PartnerSample: Boolean,
                             BidBelowFloorExceptedSource: Int,
                             FullPush: Boolean
                           )

case class PlutusLog(
                      Mu: Float,
                      Sigma: Float,
                      GSS: Double,
                      AlternativeStrategyPush: Double
                    )

case class PredictiveClearingStrategy(
                                       Model: String,
                                       Strategy: Int
                                     )

object PlutusLogsDataset {
  val S3PATH = "s3://ttd-identity/datapipeline/prod/pcresultslog/v=2/"

  def S3PATH_GEN = (dateTime: LocalDateTime) => {
    f"date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}"
  }
}