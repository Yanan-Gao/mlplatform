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

import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions


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
  val inputIntCols = config.getTupleSeq("inputIntCols" , Seq(
    ("RenderingContext",  (3+1+1)),
    // ("MatchedFoldPosition",  (3+1+1)),
    // ("VolumeControlPriority",  (5+1+1)),
    ("UserHourOfWeek",  (24*7+1+1)),
    ("AdsTxtSellerType",  (4+1+1)),
    ("PublisherType",  (4+1+1)),
    // ("InternetConnectionType",  (3+1+1)), // need to handle nulls
    ("DeviceType",  (6+1+1)),
    ("OperatingSystemFamily", (7+1+1)),
    ("Browser",  (15+1+1))
  ))

  val inputCatCols = config.getTupleSeq("inputCatCols", Seq(
    ("SupplyVendor", 100),
    ("DealId", 5000),
    ("SupplyVendorPublisherId", 15000),
    ("SupplyVendorSiteId", 100),
    ("Site", 350000),
    ("AdFormat", 100),
    // ("MatchedCategory", 4000),
    ("ImpressionPlacementId", 100),
    // ("Carrier", 200),
    ("Country", 250), // there are 195! https://www.worldometers.info/geography/how-many-countries-are-there-in-the-world/
    ("Region", 4000),
    ("Metro", 300),
    ("City", 75000), // suggestion of 10000 but this is a lot higher
    ("Zip", 90000),
    ("DeviceMake", 1000),
    ("DeviceModel", 10000),
    ("RequestLanguages", 500) // suggested 6500! Probably not all on the internet
  )

  )
  val rawCols = config.getStringSeq("rawCols" , Seq(
    "Latitude",
    "Longitude",
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

    val selectionTabular = getHashedCatCols(inputCatCols) ++ getHashedIntCols(inputIntCols) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))
    val ttv = Seq((train, "train") , (test, "test"), (validation, "validation"))

   ttv.foreach{case (df, dtype) =>
      TrainingDataTransform.writeModelInputData(df, outputPath, outputPrefix, ttdEnv, None, date, Some(daysOfDat), dtype , "tfrecord", selectionTabular)
    }
    ttv.foreach{case (df, dtype) =>
      TrainingDataTransform.writeModelInputData(df, outputPath, outputPrefix, ttdEnv, None, date, Some(daysOfDat), dtype , "parquet", selectionTabular)
    }
    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }
}
