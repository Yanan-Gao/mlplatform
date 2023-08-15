package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.schemas.{BidRequestDataset, ModelFeature}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, shiftModUdf}
import com.thetradedesk.kongming.features.Features.modelFeatures
import com.thetradedesk.kongming.datasets.{BidsImpressionsSchema, OnlineLogsDataset, OnlineLogsDiscrepancyDataset, OnlineLogsDiscrepancyRecord}
import com.thetradedesk.kongming.date
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.streaming.records.rtb.bidrequest.BidRequestRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, struct, to_json, udf, xxhash64}
import io.circe.parser.parse
import io.circe.Decoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions

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
}

object OnlineLogsDiscrepancyCheck {
  def getUnsampledBidImpressions(date: LocalDate, joinCols: Seq[String]): DataFrame = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))
      .withColumn("Browser", col("Browser.value"))
      .withColumn("InternetConnectionType", col("InternetConnectionType.value"))
      .withColumn("DeviceType", col("DeviceType.value"))
      .withColumn("OperatingSystem", col("OperatingSystem.value"))
      .withColumn("Latitude", (col("Latitude") + lit(90.0)) / lit(180.0))
      .withColumn("Longitude", (col("Longitude") + lit(180.0)) / lit(360.0))
      .na.fill(0, Seq("Latitude", "Longitude"))

    val cols = bidImpressions.columns.map(_.toLowerCase).toSet
    val features = modelFeatures.filter(x => cols.contains(x.name.toLowerCase))
    val joinFeatures = joinCols.map(x => ModelFeature(x, "", None, 1))

    features.foldLeft(bidImpressions.select((joinFeatures ++ features).map(x => col(x.name)): _*)) { (df, f) =>
      val c = f.cardinality.getOrElse(1)

      if (c != 1) df.withColumn(f.name, shiftModUdf(xxhash64(col(f.name)), lit(c)))
      else df
    }
  }

  def getOnlineLogs(date: LocalDate, modelName: String): DataFrame = {
    val logs = OnlineLogsDataset.readOnlineLogsDataset(date, modelName)

    val featureNames = modelFeatures.map(x => x.name)
    val dfParsedJson = logs.withColumn("ParsedJson", OnlineLogsParser.parseFeatureJsonUDF(col("Features")))
    featureNames.foldLeft(dfParsedJson)((df, f) => df.withColumn(f, OnlineLogsParser.extractFeatureUDF(col("ParsedJson"), lit(f))))
      .drop("Features", "DynamicFeatures", "ParsedJson")
      .na.drop()
  }

  def getBidRequests(date: LocalDate): DataFrame = {
    loadParquetData[BidRequestRecord](s3path = BidRequestDataset.BIDSS3, date = date)
      .select(col("AvailableBidRequestId"), col("BidRequestId"))
  }

  def getDiscrepancy(onlineLogs: DataFrame, bidImpressionLogs: DataFrame): DataFrame = {
    val f1 = onlineLogs.columns.toSet
    val f2 = bidImpressionLogs.columns.toSet

    val featureCols = f1.intersect(f2).toList

    val onlineFeatures = onlineLogs.select(featureCols.map(col): _*)
    val bidImpressionFeatures = bidImpressionLogs.select(featureCols.map(col): _*)

    onlineFeatures.as("a")
      .join(bidImpressionFeatures.as("b"), Seq("BidRequestId"), "inner")
      .filter(featureCols.map(x => s"a.$x != b.$x").mkString(" OR ")) // create filter condition based on featureCols
      .select(col("name"),
        to_json(struct(col("a.*"))).as("OnlineFeatures"),
        to_json(struct(col("b.*"))).as("BidImpressionFeatures")
      )
  }

  def main(args: Array[String]): Unit = {
    val modelName = config.getStringRequired("modelName")

    val unsampledBidImpressions = getUnsampledBidImpressions(date, Seq("BidRequestId"))
    val onlineLogs = getOnlineLogs(date, modelName)
    val bidRequests = getBidRequests(date)

    val bidImpressionLogs = onlineLogs.select("BidRequestId")
      .join(bidRequests, Seq("BidRequestId"), "left")
      .join(unsampledBidImpressions, Seq("AvailableBidRequestId"), "left")

    val logDiscrepancy = getDiscrepancy(onlineLogs, bidImpressionLogs).selectAs[OnlineLogsDiscrepancyRecord]
    if (!logDiscrepancy.isEmpty) {
      OnlineLogsDiscrepancyDataset(modelName).writePartition(logDiscrepancy, date, None)
    }

    spark.stop()
  }
}
