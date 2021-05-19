package job

import com.thetradedesk.data._
import com.thetradedesk.data.load.TfRecordWriter
import com.thetradedesk.data.transform.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import java.time.LocalDate


object CleanInputDataProcessor {
  val date = config.getDate("date" , LocalDate.now()) //TODO: note this can cause bad things. Date should be required and not linked ot current date.
  val lookBack = config.getInt("daysOfDat" , 1)

  val inputPath = config.getString("inputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/")
  val inputPrefix = config.getString("inputPrefix" , "raw")
  val outputPrefix = config.getString("outputPrefix" , "clean")
  val svName = config.getString("svName", "google")
  val extremeValueThreshold = config.getDouble("mbwRatio" , 0.8)

  val folderName = config.getString("folderName" , "clean/google")
  val tfRecordPath = config.getString("tfRecodPath" , "/tfrecord/")
  val dims = config.getInt("dims" , 500000)
  val inputIntCols = config.getStringSeq("inputIntCols" , Seq(
    "RenderingContext",
    "MatchedFoldPosition",
    "VolumeControlPriority",
    "UserHourOfWeek",
    "AdsTxtSellerType",
    "PublisherType",
    "InternetConnectionType", // need to handle nulls
    "DeviceType",
    "OperatingSystemFamily",
    "Browser"
  ))
  val inputCatCols = config.getStringSeq("inputCatCols", Seq(
    "SupplyVendor",
    "DealId",
    "SupplyVendorPublisherId",
    "SupplyVendorSiteId",
    "Site",
    "AdFormat",
    "MatchedCategory",
    "ImpressionPlacementId",
    "Carrier" ,
    "Country",
    "Region",
    "Metro",
    "City",
    "Zip",
    "DeviceMake",
    "DeviceModel",
    "RequestLanguages"
  )
  )
val rawCols = config.getStringSeq("rawCols" , Seq(
    "Latitude",
    "Longitude",
    "sin_hour_day",
    "cos_hour_day",
    "sin_hour_week",
    "cos_hour_week",
    "sin_minute_hour",
    "cos_minute_hour",
    "sin_minute_day",
    "cos_minute_day"
  )
  )

  val targets = config.getStringSeq("targets" , Seq(
    "is_imp",
    "AuctionBidPrice",
    "RealMediaCost",
    "mb2w",
    "FloorPriceInUSD"
  )
  )

  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_processor_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val totalData = prometheus.createGauge("clean_data_row_count" , "count of processed rows")
  val mbwDataCount = prometheus.createGauge("total_mb2w_data" , "Total Data with MB2W")
  val mbwBidsCount = prometheus.createGauge("bids_with_mb2w" , "Total Bids with MB2W")
  val mbwImpsCount = prometheus.createGauge("impressions_with_mb2w" , "Total Imps with MB2W")
  val mbwValidBidsCount = prometheus.createGauge("bids_with_valid_mb2w", "Total Bids with Valid MB2W")
  val mbwValidImpsCount = prometheus.createGauge("impressions_with_valid_mb2w", "Total Impressions with Valid MB2W")


  def main(args: Array[String]): Unit  = {

    val df = CleanInputData.createCleanDataframe(inputPath, inputPrefix, date, extremeValueThreshold)

    df
      .repartition(200)
      .write.mode(SaveMode.Overwrite)
      .parquet(CleanInputData.cleanDataS3Path(outputPath, outputPrefix, date, Some(svName)))


    totalData.set(df.cache().count)
    mbwDataCount.set(df.filter(col("mb2w").isNotNull).count)
    mbwBidsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).count)
    mbwImpsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).count)
    mbwValidBidsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).filter(col("mb2w") >= col("b_RealBidPrice")).count)
    mbwValidImpsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).filter(col("mb2w") <= col("RealMediaCost")).count)


    // create TFRecord data

    val allInputCols = inputCatCols ++ inputIntCols

    val selectionTabular = inputCatCols.map(a => col(a)).toArray ++ inputIntCols.map(a => col(a)) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))

    val selectionHash = Array(
      vec_indices(col("features")).alias("i"),
      vec_size(col("features")).alias("s"),
      vec_values(col("features")).alias("v"),
    ) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))

    val tfr = new TfRecordWriter
    val feat = tfr.hashData(df, allInputCols)

    tfr.writeData(feat, selectionHash, date, folderName, tfRecordPath, "hash")
    tfr.writeData(df, selectionTabular, date, folderName, tfRecordPath, "tabular")

  }
}
