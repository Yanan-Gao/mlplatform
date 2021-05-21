package job

import com.thetradedesk.data._
import com.thetradedesk.data.load.TfRecordWriter
import com.thetradedesk.data.transform.TrainingDataTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import java.time.LocalDate


object ModelInputProcessor extends Logger {

  val ttdEnv = config.getString("ttd.env" , "dev")
  val date = config.getDate("date" , LocalDate.now())
  val daysOfDat = config.getInt("daysOfDat" , 1)
  val svName = config.getString("svName", "google")
  val inputPath = config.getString("inputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val inputPrefix = config.getString("inputPrefix" , "clean")

  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val outputPrefix = config.getString("outputPrefix" , "modelinput")

  val tfRecordPath = config.getString("tfRecodPath" , "tfrecord")
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
  val jobDurationTimer = prometheus.createGauge("training_model_input_runtime", "Time to process 1 day of clean data in to model input data").startTimer()
  val trainCount = prometheus.createGauge("train_row_count", "rows of train data")
  val validationCount = prometheus.createGauge("validation_row_count" , "rows of validation data")
  val testCount = prometheus.createGauge("test_row_count" , "ros of test data")


  def main(args: Array[String]): Unit = {

    //TODO: TFRecord create TF record from the train/test/val dataframe

    // create TFRecord data

    val cleanPaths = TrainingDataTransform.inputDataPaths(inputPath, inputPrefix, ttdEnv, None, date, Some(daysOfDat))

    val (train, validation, test) = TrainingDataTransform.createComboSplits(cleanPaths)

    trainCount.set(train.cache.count())
    validationCount.set(validation.cache.count())
    testCount.set(test.cache.count())

    TrainingDataTransform.writeModelInputParquetData(train, outputPath, outputPrefix, ttdEnv, None, date, Some(daysOfDat), "train")
    TrainingDataTransform.writeModelInputParquetData(test, outputPath, outputPrefix, ttdEnv, None, date, Some(daysOfDat), "test")
    TrainingDataTransform.writeModelInputParquetData(validation, outputPath, outputPrefix, ttdEnv, None, date, Some(daysOfDat), "validation")


    //    val allInputCols = inputCatCols ++ inputIntCols
//
//    val selectionTabular = inputCatCols.map(a => col(a)).toArray ++ inputIntCols.map(a => col(a)) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))
//
//    val selectionHash = Array(
//      vec_indices(col("features")).alias("i"),
//      vec_size(col("features")).alias("s"),
//      vec_values(col("features")).alias("v"),
//    ) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))
//
//    val feat = TfRecordWriter.hashData(df.toDF, allInputCols, dims)
//
//
//
//    TfRecordWriter.writeData(feat, selectionHash, date, outputPath, inputPrefix, ttdEnv, tfRecordPath, "hash")
//    TfRecordWriter.writeData(df.toDF, selectionTabular, date, outputPath, inputPrefix, ttdEnv, tfRecordPath, "tabular")
//

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
