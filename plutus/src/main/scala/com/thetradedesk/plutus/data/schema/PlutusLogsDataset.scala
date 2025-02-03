package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.utils.{S3HourlyParquetDataset, localDatetimeToTicks}
import com.thetradedesk.plutus.data.{envForWrite, paddedDatePart, utils}
import com.thetradedesk.protologreader.protoformat.PredictiveClearingResults
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import com.thetradedesk.spark.util.protologreader.S3ObjectFinder.getS3ObjectPathFromDirectory
import com.thetradedesk.spark.util.protologreader.{ProtoLogReader, S3Client, S3PathGenerator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import java.time.{LocalDate, LocalDateTime}

case class PlutusLogsData(
                           BidRequestId: String,

                           LogEntryTime: Long,
                           IsValuePacing: Boolean,
                           AuctionType: Int,
                           DealId: String,

                           SupplyVendor: String,
                           AdgroupId: String,

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

                           UseUncappedBidForPushdown:Boolean,
                           UncappedFirstPriceAdjustment:Double,

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
  def transformPcResultsRawLog(dataset: Dataset[PcResultsRawLogs], hour: LocalDateTime): Dataset[PlutusLogsData] = {
    dataset.where(
        $"LogEntryTime" >= localDatetimeToTicks(hour) and
          $"LogEntryTime" < localDatetimeToTicks(hour.plusHours(1)))
    .select(
      "PlutusLog.*",
      "PredictiveClearingStrategy.*",
      "*"
    )
    .selectAs[PlutusLogsData]
  }

  def loadPlutusLogData(dateTime: LocalDateTime): Dataset[PlutusLogsData] = {
    val pathGenerator = new S3Client("thetradedesk-useast-logs-2" , "predictiveclearingresults/collected")
    val logReader = new ProtoLogReader[PredictiveClearingResults.PcResultLog](
      cloudClient = pathGenerator,
      parseFunc = PredictiveClearingResults.PcResultLog.parseFrom,
      sparkSession = spark
    )

    // An hour h's data is spread between h-1 and h so we get
    // the files for this hour and the last one so we can filter only this hours data later
    val jodaDateTime = utils.javaToJoda(dateTime);
    val files = (pathGenerator.getSpecificHourAvailsStreamFiles(jodaDateTime.minusHours(1)) ++
      pathGenerator.getSpecificHourAvailsStreamFiles(jodaDateTime)).flatMap(getS3ObjectPathFromDirectory)

    val pcResultData = logReader.readSpecificFiles(files)
      .map(i => PcResultsRawLogs(
        utils.uuidFromLongs( i.getBidRequestId.getLo, i.getBidRequestId.getHi),
        i.getInitialBid,
        i.getFinalBidPrice,
        i.getDiscrepancy,
        i.getBaseBidAutoOpt,
        i.getLegacyPcPushdown,
        PlutusLog(i.getPlutusLog.getMu, i.getPlutusLog.getSigma, i.getPlutusLog.getGSS, i.getPlutusLog.getAlternativeStrategyPush),
        PredictiveClearingStrategy(i.getPredictiveClearingStrategy.getModel, i.getPredictiveClearingStrategy.getStrategy),
        i.getOptOutDueToFloor,
        i.getFloorPrice,
        i.getPartnerSample,
        i.getBidBelowFloorExceptedSource,
        i.getFullPush,
        i.getFloorBufferAdjustment,
        i.getUseUncappedBidForPushdown,
        i.getUncappedFirstPriceAdjustment,
        i.getLogEntryTime,
        i.getSupplyVendor,
        i.getAdgroupId,
        i.getIsValuePacing,
        i.getAuctionType,
        i.getDealId,
      )).toDS()

    transformPcResultsRawLog(pcResultData, dateTime)
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
                             FullPush: Boolean,
                             FloorBufferAdjustment: Double,
                             UseUncappedBidForPushdown: Boolean,
                             UncappedFirstPriceAdjustment: Double,
                             LogEntryTime: Long,
                             SupplyVendor: String,
                             AdgroupId: String,
                             IsValuePacing: Boolean,
                             AuctionType: Int,
                             DealId: String
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

object PlutusOptoutBidsDataset extends S3HourlyParquetDataset[PlutusLogsData]{
  override protected def genHourSuffix(datetime: LocalDateTime): String =
    f"hour=${datetime.getHour}"

  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String =
    f"s3://ttd-identity/datapipeline/${env}/pc_optout_bids/v=1"

  /* DEPRECATED */
  @deprecated def S3PATH_DATE_GEN = (date: LocalDate) => {
    f"date=${paddedDatePart(date)}"
  }

  @deprecated def S3PATH_GEN = (dateTime: LocalDateTime) => {
    f"date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}"
  }

  @deprecated def S3PATH_BASE = (env: Option[String]) => {
    f"s3://ttd-identity/datapipeline/${env.getOrElse(envForWrite)}/pc_optout_bids/v=1/"
  }

  @deprecated def S3PATH_FULL_HOUR = (dateTime: LocalDateTime, env: Option[String]) => {
    f"${S3PATH_BASE(env)}${S3PATH_GEN(dateTime)}"
  }
}