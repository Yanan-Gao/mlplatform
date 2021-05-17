package job

import java.time.LocalDate

import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.linalg.{SparseVector, Vector}


object TrainDataProcessor {
  val date = config.getDate("date" , LocalDate.now())
  val lookBack = config.getInt("daysOfDat" , 1)
  val svName = config.getString("svName", "google")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/trainingdata")
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

    createCleanDataset(outputPath, folderName)

    val df = spark.read.parquet(outputPath + folderName + "/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")
    val feat = hashData(df)

    writeHashedData(feat)
    writeTabularData(df)

  }

  def createCleanDataset(dataRoot: String, folderName: String) = {

    val df = spark.read.parquet(s"$outputPath/$svName/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))
    // need this as was int in previous query - making it a float will make things easier later on

    val df_clean = df
      .filter((col("mb2w").isNotNull))
      .withColumn("valid",
        // there are cases that dont make sense - we choose to remove these for simplicity while we investigate further.

        // impressions where MB2W < Media Cost and MB2W is above a floor (if present)
        when((
          (col("is_imp") === 1.0) &&
            (col("mb2w") <= col("RealMediaCost")) &&
            ((col("mb2w") >= col("FloorPriceInUSD")) || col("FloorPriceInUSD").isNullOrEmpty )
          ), true)
          // Bids where MB2W is > bid price AND not an extreme value AND is above floor (if present)
          .when((
            (col("is_imp") === 0.0) &&
              (col("mb2w") > col("b_RealBidPrice")) &&
              ((col("FloorPriceInUSD").isNullOrEmpty) || (col("mb2w") >= col("FloorPriceInUSD"))) &&
              (round(col("b_RealBidPrice") / col("mb2w"), 1) > 0.8) &&
              ((col("mb2w") > col("FloorPriceInUSD")) || (col("FloorPriceInUSD").isNullOrEmpty))
            ), true)
          .otherwise(false)
      )
      .filter(col("valid") === true)
      .drop("valid")

    totalData.set(df_clean.cache().count)
    mbwDataCount.set(df_clean.filter(col("mb2w").isNotNull).count)
    mbwBidsCount.set(df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).count)
    mbwImpsCount.set(df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).count)
    mbwValidBidsCount.set(df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).filter(col("mb2w") >= col("b_RealBidPrice")).count)
    mbwValidImpsCount.set(df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).filter(col("mb2w") <= col("RealMediaCost")).count)

    /*println(f"Total Data: ${df_clean.count}")
    println(f"Total Data with MB2W: ${df_clean.filter(col("mb2w").isNotNull).count}")
    println(f"Total Bids with MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).count}")
    println(f"Total Imps with MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).count}")
    println(f"Total Bids with Valid MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).filter(col("mb2w") >= col("b_RealBidPrice")).count}")
    println(f"Total Imps with Valid MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).filter(col("mb2w") <= col("RealMediaCost")).count}")
*/
    println("ouputting clean data to:\n" + s"$dataRoot/$folderName/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")

    df_clean.repartition(200).write.mode(SaveMode.Overwrite).parquet(s"$dataRoot/$folderName/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")

  }

  def hashData(df: DataFrame) = {

    val allInputCols = inputCatCols.toArray ++ inputIntCols.toArray
    val hasher = new FeatureHasher()
      .setNumFeatures(dims)
      .setInputCols(allInputCols)
      .setCategoricalCols(allInputCols)
      .setOutputCol("features")
    hasher.transform(df)

  }

  def writeHashedData(featurised: DataFrame): Unit = {
    val vec_size = udf((v: Vector) => v.size)
    val vec_indices = udf((v: SparseVector) => v.indices)
    val vec_values = udf((v: SparseVector) => v.values)

    val selection = Array(
      vec_indices(col("features")).alias("i"),
      vec_size(col("features")).alias("s"),
      vec_values(col("features")).alias("v"),
    ) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))

    featurised
      .select(selection: _*)
      .repartition(75)
      .write.format("tfrecords").option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite").save(outputPath + folderName + tfRecordPath + "hashing/" + f"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")

  }

  def writeTabularData(df: DataFrame): Unit = {
    val selectionTabular = inputCatCols.map(a => col(a)).toArray ++ inputIntCols.map(a => col(a)) ++ rawCols.map(a => col(a)) ++ targets.map(a => col(a))

    df
      .select(selectionTabular: _*)
      .repartition(200)
      .write.format("tfrecords").option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite").save(outputPath + folderName + tfRecordPath + "tabular/" + f"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")
  }



}
