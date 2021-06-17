package job


import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.time.LocalDate

import com.thetradedesk.plutus.data.plutusDataPath
import com.thetradedesk.plutus.data.transform.CleanInputDataTransform


object CleanInputDataProcessor {
  val date = config.getDate("date" , LocalDate.now()) //TODO: note this can cause bad things. Date should be required and not linked ot current date.
  val lookBack = config.getInt("daysOfDat" , 1)

  val inputPath = config.getString("inputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc")
  val inputPrefix = config.getString("inputPrefix" , "raw")
  val outputPrefix = config.getString("outputPrefix" , "clean")
  val svName = config.getString("svName", "google")
  val extremeValueThreshold = config.getDouble("mbwRatio" , 0.8)
  val ttdEnv = config.getString("ttd.env" , "dev")


  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_processor_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val totalData = prometheus.createGauge("clean_data_row_count" , "count of processed rows")
  val mbwDataCount = prometheus.createGauge("total_mb2w_data" , "Total Data with MB2W")
  val mbwBidsCount = prometheus.createGauge("bids_with_mb2w" , "Total Bids with MB2W")
  val mbwImpsCount = prometheus.createGauge("impressions_with_mb2w" , "Total Imps with MB2W")
  val mbwValidBidsCount = prometheus.createGauge("bids_with_valid_mb2w", "Total Bids with Valid MB2W")
  val mbwValidImpsCount = prometheus.createGauge("impressions_with_valid_mb2w", "Total Impressions with Valid MB2W")
  val cleanDataOutputCount = prometheus.createGauge("clean_input_data_count" , "total rows of clean input data written to s3 in parquet")


  def main(args: Array[String]): Unit  = {

    val df = CleanInputDataTransform.createCleanDataset(inputPath, ttdEnv, inputPrefix, date, extremeValueThreshold, Some(svName), cleanDataOutputCount)

    df
      .repartition(200)
      .write.mode(SaveMode.Overwrite)
      .parquet(plutusDataPath(outputPath, ttdEnv, outputPrefix, Some(svName), date))

    totalData.set(df.cache().count)
    mbwDataCount.set(df.filter(col("mb2w").isNotNull).count)
    mbwBidsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).count)
    mbwImpsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).count)
    mbwValidBidsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).filter(col("mb2w") >= col("b_RealBidPrice")).count)
    mbwValidImpsCount.set(df.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).filter(col("mb2w") <= col("RealMediaCost")).count)

  }
}
