package job

import com.google.protobuf.Field.Cardinality
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.{BidRequestDataset, ModelFeature}
import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, GERONIMO_DATA_SOURCE, intModelFeaturesCols, loadParquetData, shiftModUdf}
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, modelFeatures, rawModelFeatureCols, rawModelFeatureNames, seqFields}
import com.thetradedesk.kongming.datasets.{BidsImpressionsSchema, OnlineLogsDataset, OnlineLogsDiscrepancyDataset, OnlineLogsDiscrepancyRecord}
import com.thetradedesk.kongming.{KongmingApplicationName, LogsDiscrepancyCountGaugeName, RunTimeGaugeName, date, getJobNameWithExperimentName}
import com.thetradedesk.kongming.transform.ContextualTransform
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.streaming.records.rtb.bidrequest.BidRequestRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, concat, lit, struct, to_json, udf, xxhash64}
import io.circe.parser.parse
import io.circe.Decoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object OnlineLogsParser extends Serializable {
  case class FeatureDimensions(Dimensions: List[Int])
  case class OnlineLogFeatureJson(Array: List[Float], Shape: FeatureDimensions)

  implicit val featureDimensionsDecoder: Decoder[FeatureDimensions] = Decoder.forProduct1("Dimensions")(FeatureDimensions.apply)
  implicit val onlineLogFeatureJsonDecoder: Decoder[OnlineLogFeatureJson] = Decoder.forProduct2("Array", "Shape")(OnlineLogFeatureJson.apply)

  def parseFeatureJsonUDF: UserDefinedFunction = udf((featureJsonStr: String) => {
    val json = parse(featureJsonStr).getOrElse(null)
    json.as[Map[String, OnlineLogFeatureJson]].getOrElse(null)
  })

  def extractFeatureUDF: UserDefinedFunction = udf((featuresParsed: Map[String, OnlineLogFeatureJson], feature: String) => {
    val featureValue = featuresParsed.getOrElse(feature, null)
    featureValue.Array.head
  })

  def extractFeatureArrayUDF: UserDefinedFunction = udf((featuresParsed: Map[String, OnlineLogFeatureJson], feature: String) => {
    val featureValue = featuresParsed.getOrElse(feature, null)
    val shape = featureValue.Shape.Dimensions(1)

    featureValue.Array.slice(0, shape)
  })
}

object OnlineLogsDiscrepancyCheck {
  def getUnsampledBidImpressions(date: LocalDate): DataFrame = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val rawBidImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("AdFormat", concat(col("AdWidthInPixels"), lit('x'), col("AdHeightInPixels")))
      .withColumn("Browser", col("Browser.value"))
      .withColumn("InternetConnectionType", col("InternetConnectionType.value"))
      .withColumn("DeviceType", col("DeviceType.value"))
      .withColumn("OperatingSystem", col("OperatingSystem.value"))
      .withColumn("RenderingContext", col("RenderingContext.value"))
      .na.fill(0, Seq("Latitude", "Longitude"))

    val contextualFeatures = ContextualTransform.generateContextualFeatureTier1(
      rawBidImpressions.select("BidRequestId", "ContextualCategories").dropDuplicates("BidRequestId").selectAs[ContextualData]
    ).drop("ContextualCategoryNumberTier1")

    val bidImpressions = rawBidImpressions.join(contextualFeatures, Seq("BidRequestId"), "left")

    val cols = bidImpressions.columns.map(_.toLowerCase).toSet
    val features = modelFeatures.filter(x => cols.contains(x.name.toLowerCase) && !seqFields.contains(x))
    val joinFeatures = Seq("BidRequestId").map(x => ModelFeature(x, "", None, 1))

    val tensorflowSelectionTabular = intModelFeaturesCols(features) ++ rawModelFeatureCols(joinFeatures) ++ aliasedModelFeatureCols(seqFields)
    bidImpressions.select(tensorflowSelectionTabular:_*)
  }

  def getOnlineLogs(date: LocalDate, joinCols: Seq[String], modelName: String): DataFrame = {
    val logs = OnlineLogsDataset.readOnlineLogsDataset(date, modelName)

    val joinFeatures = joinCols.map(x => ModelFeature(x, "", None, 1))
    val tensorflowSelectionTabular = rawModelFeatureNames(modelFeatures).map(x => col(x.toString)) ++ rawModelFeatureCols(joinFeatures) ++ aliasedModelFeatureCols(seqFields)

    val dfParsedJson = logs.withColumn("ParsedJson", OnlineLogsParser.parseFeatureJsonUDF(col("Features")))
    modelFeatures.foldLeft(dfParsedJson)((df, f) => {
      f.dtype match {
        case ARRAY_INT_FEATURE_TYPE => df.withColumn(f.name, OnlineLogsParser.extractFeatureArrayUDF(col("ParsedJson"), lit(f.name)))
        case _ => df.withColumn(f.name, OnlineLogsParser.extractFeatureUDF(col("ParsedJson"), lit(f.name)))
      }
    }).na.drop()
      .select(tensorflowSelectionTabular:_*)

  }

  def getBidRequests(date: LocalDate): DataFrame = {
    loadParquetData[BidRequestRecord](s3path = BidRequestDataset.BIDSS3, date = date)
      .select(col("AvailableBidRequestId"), col("BidRequestId"))
  }

  def getDiscrepancy(onlineLogs: DataFrame, bidImpressionLogs: DataFrame, precision: Double, ignoreCols: Seq[String]): DataFrame = {
    val f1 = onlineLogs.columns.toSet
    val f2 = bidImpressionLogs.columns.toSet

    val featureCols = f1.intersect(f2).filter(x => !ignoreCols.contains(x)).toList

    val onlineFeatures = onlineLogs.select(featureCols.map(col): _*)
    val bidImpressionFeatures = bidImpressionLogs.select(featureCols.map(col): _*)

    onlineFeatures.as("a")
      .join(bidImpressionFeatures.as("b"), Seq("BidRequestId"), "inner")
      .filter(featureCols.map(x => s"abs(a.$x - b.$x) > $precision").mkString(" OR ")) // create filter condition based on featureCols
      .select(col("BidRequestId"),
        to_json(struct(col("a.*"))).as("OnlineFeatures"),
        to_json(struct(col("b.*"))).as("BidImpressionFeatures")
      )
  }

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("OnlineLogsDiscrepancyCheck"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(LogsDiscrepancyCountGaugeName, "Number of rows written", "ModelName")

    val modelName = config.getStringRequired("modelName")
    val precision = config.getDouble("precision", 0.0001)

    val unsampledBidImpressions = getUnsampledBidImpressions(date)

    val bidRequests = getBidRequests(date)

    val onlineLogs = getOnlineLogs(date, Seq("AvailableBidRequestId"), modelName)
      .cache

    val bidRequestToImpression = bidRequests
      .join(broadcast(onlineLogs), Seq("AvailableBidRequestId"), "left_semi")
      .cache

    val bidImpressionLogs = unsampledBidImpressions.join(broadcast(bidRequestToImpression), Seq("BidRequestId"), "left_semi")
      .cache

    val ignoreCols = Seq("AdFormat") // Online Logs have incorrect ad format. We can ignore it temporarily until online logs are fixed
    val logDiscrepancy = getDiscrepancy(onlineLogs.join(bidRequestToImpression, Seq("AvailableBidRequestId"), "left"), bidImpressionLogs, precision, ignoreCols)
      .selectAs[OnlineLogsDiscrepancyRecord]

    val (discrepancyRowName, discrepancyRowCount) = OnlineLogsDiscrepancyDataset(modelName).writePartition(logDiscrepancy, date, None)
    outputRowsWrittenGauge.labels(modelName).set(discrepancyRowCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
