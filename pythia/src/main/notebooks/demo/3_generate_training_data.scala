// Databricks notebook source
dbutils.widgets.text("date_to_process", "2023-07-19")
dbutils.widgets.text("drop_power_users", "1000")
dbutils.widgets.text("make_test_set", "false")
dbutils.widgets.text("num_users", "1e6")
dbutils.widgets.text("min_rows_per_label", "16667")
dbutils.widgets.text("run_id", "0000")

// COMMAND ----------

import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import org.apache.spark.sql.functions._
import java.time.LocalDate

import com.thetradedesk.geronimo.shared.{shiftMod, shiftModUdf}

// COMMAND ----------

// MAGIC %run "./pythia_module"

// COMMAND ----------

val fromDate = LocalDate.parse(dbutils.widgets.get("date_to_process"))
val dpu = dbutils.widgets.get("drop_power_users")
val mts = dbutils.widgets.get("make_test_set")
val nu = dbutils.widgets.get("num_users")
val mr = dbutils.widgets.get("min_rows_per_label")
val run_id = dbutils.widgets.get("run_id")

val record_type = if (mts == "true") "unresampled" else s"dpu=$dpu/nu=$nu/mr=$mr"

// val basePath = "s3://thetradedesk-useast-hadoop/Data_Science/bryan/DATPERF-2811"
val basePath = "s3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/v=1/dev"
val demoPath = s"$basePath/owdi_preprocess/$run_id/"
val dataPath = s"$basePath/scratch/$record_type/Date="+fromDate.toString()
val outputPath = s"$basePath/tfrecords/$record_type/"

val presetPartitions = if (mts == "true") 512 else 64
val filteredPartitions = config.getInt("filteredPartitions", presetPartitions)

// COMMAND ----------

dataPath

// COMMAND ----------

// $ is shortcut for column in spark
val seedData = spark.read.parquet(dataPath).drop($"hourPart")
val (seedAgeGender, labelCounts) = ModelInputTransform.transform(seedData)

// COMMAND ----------

display(seedAgeGender.groupBy("Country").count())

// COMMAND ----------

writeData(seedAgeGender, outputPath, "agegender", fromDate, filteredPartitions)

// COMMAND ----------

display(labelCounts)
writeData(labelCounts, outputPath, "labelcounts", fromDate, 1, false)

// COMMAND ----------

val countrySummary = (seedData.select("Country").distinct()
.withColumn("countryHash", xxhash64(col("Country")))
.withColumn("mapped", shiftModUdf(col("countryHash"), lit(252))))
display(countrySummary)
writeData(countrySummary, outputPath, "country_hash", fromDate, 1, false)

// COMMAND ----------

// clean up intermediate files (rebalanced files in parquet format, separated by hourPart)
dbutils.fs.rm(demoPath, true)
dbutils.fs.rm(dataPath, true)
