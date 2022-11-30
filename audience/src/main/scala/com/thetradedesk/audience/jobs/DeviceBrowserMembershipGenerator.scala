package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, ttdEnv, shouldConsiderTDID2}
import com.thetradedesk.audience.datasets.{FirstPartyPixelModelInputRecord, S3Roots, SeenInBiddingV2DeviceDataSet}
import com.thetradedesk.audience.transform.ModelFeatureTransform
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData, shiftMod}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.sql.SQLFunctions._

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Browser extends Enumeration {
  type Type = Value
  val None, Other, IE7, IE8, IE9, Firefox, Chrome, Safari, Opera, IE10, IE11, InApp, Edge, Baidu, Yandex, WebView = Value
}

object OSFamily extends Enumeration {
  type Type = Value
  val None, Other, Windows, OSX, Linux, iOS, Android, WindowsPhone = Value
}

object Utils {
  def downSampleByBrowserOperatingSystemFamily(df: Dataset[_], numBuckets: Int, sampledBuckets: Int, isDataHashed: Boolean) = {
    val chrome = if (isDataHashed) shiftMod(Browser.Chrome.id, ModelFeatureTransform.tryGetFeatureCardinality[FirstPartyPixelModelInputRecord]("Browser")) else Browser.Chrome.id
    val android = if (isDataHashed) shiftMod(OSFamily.Android.id, ModelFeatureTransform.tryGetFeatureCardinality[FirstPartyPixelModelInputRecord]("OperatingSystemFamily")) else OSFamily.Android.id

    df.withColumn("Unaffected", 'Browser =!= lit(chrome) && 'OperatingSystemFamily =!= lit(android))
      .filter('Unaffected || abs(xxhash64(concat('TDID, lit("AeDecemberTest")))) % numBuckets < lit(sampledBuckets))
      .drop("Unaffected")
  }
}

object DeviceBrowserMembershipGenerator {
  object Config {
    var bidsImpressionLookBack = config.getInt("bidsImpressionLookBack", default=6)
    var idRetentionNumBuckets = config.getInt("idRetentionNumBuckets", default=10)
    var idRetentionSampledBucketsRaw = config.getString("idRetentionSampledBuckets", default="0,2,5,10")
    lazy val idRetentionSampledBuckets = idRetentionSampledBucketsRaw.split(",").map(_.toInt)
    var goalTargetingDataIdsRaw = config.getString("goalTargetingDataIds", default="285682407,248037918")
    lazy val goalTargetingDataIds = spark.sparkContext.broadcast(goalTargetingDataIdsRaw.split(",").map(_.toLong))
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val multiverse = generateMultiverse(date)

    val downSampledPixels = downSamplePixels(date, multiverse)

    downSampledPixels.map({
      case (sampledBucket, downSampledPixel) =>
        Config.goalTargetingDataIds.value.map(targetingDataId => downSampledPixel.filter(array_contains('TargetingDataIds, lit(targetingDataId))).select('TDID)
          .coalesce(1)
          .write.mode(SaveMode.Overwrite)
          .option("header", value = false)
          .csv(s"${S3Roots.ML_PLATFORM_ROOT}/${ttdEnv}/audience/downsampledPixels/date=${date.format(dateFormatter)}/${targetingDataId}_${sampledBucket}")
        )
    })
  }

  def generateMultiverse(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE), lookBack=Some(Config.bidsImpressionLookBack))
      .withColumnRenamed("UIID", "TDID")
      .select('TDID, 'OperatingSystemFamily, 'Browser)
      .filter(shouldConsiderTDID2('TDID))
      .withColumn("Browser", 'Browser("value"))
      .withColumn("OperatingSystemFamily", 'OperatingSystemFamily("value"))
      .distinct
      .cache

    Config.idRetentionSampledBuckets.map(sampledBucket => (sampledBucket, Utils.downSampleByBrowserOperatingSystemFamily(bidsImpressions, Config.idRetentionNumBuckets, sampledBucket, false).select('TDID))).toMap
  }

  def downSamplePixels(date: LocalDate, multiverse: Map[Int, DataFrame]) = {
    val selectedSIB = SeenInBiddingV2DeviceDataSet().readPartition(date)(spark)
      .withColumnRenamed("Tdid", "TDID")
      .filter(size('FirstPartyTargetingDataIds) > 0)
      .withColumn("TargetingDataIds", array_intersect('FirstPartyTargetingDataIds, typedLit(Config.goalTargetingDataIds.value)))
      .filter(size('TargetingDataIds) > 0)

    multiverse.map({
      case (sampledBucket, universe) => (sampledBucket, selectedSIB.join(universe, Seq("TDID"), "inner"))
    }).toMap
  }
}

object DeviceBrowserModelInputSampler {
  object Config {
    var idRetentionNumBuckets = config.getInt("idRetentionNumBuckets", default=10)
    var idRetentionSampledBuckets = config.getInt("idRetentionSampledBuckets", default=0)
    var modelInputPath = config.getString("modelInputPath", "")
    var modelOutputPath = config.getString("modelOutputPath", "")
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val inputPrefix = s"${Config.modelInputPath}/date=${date.format(dateFormatter)}"
    val outputPrefix = s"${Config.modelOutputPath}/date=${date.format(dateFormatter)}"

    for (subdir <- FSUtils.listDirectories(inputPrefix)(spark)) {
      val inputPath = inputPrefix + "/" + subdir
      val outputPath = outputPrefix + "/" + subdir

      val modelInput = spark.read.format("tfrecord")
        .option("recordType", "Example")
        .load(inputPath)

      val sampledModelInput = run(modelInput)

      sampledModelInput
        .repartition(100)
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(outputPath)
    }
  }

  def run(modelInput: DataFrame) = {
    Utils.downSampleByBrowserOperatingSystemFamily(modelInput, Config.idRetentionNumBuckets, Config.idRetentionSampledBuckets, true)
  }
}