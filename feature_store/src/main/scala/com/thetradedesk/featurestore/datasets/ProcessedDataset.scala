package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.ttdEnv
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Encoder, Encoders}

import java.time.LocalDate
import scala.reflect.runtime.universe._

abstract class ProcessedDataset[T <: Product : Manifest] extends LightReadableDataset[T] with LightWritableDataset[T] {

  val lookback: Int = 0
  val version: Int = 1
  val datasetName: String

  lazy override val dataSetPath: String =
    s"features/feature_store/${ttdEnv}/processed/${datasetName}/v=${version}" +
      (if (lookback == 0) "" else s"/lookback=${lookback}d")

  override val rootPath: String = FeatureConstants.ML_PLATFORM_S3_PATH
  override val repartitionColumn: Option[String] = Some(FeatureConstants.UserIDKey)
  override val writeThroughHdfs: Boolean = true

  val enc: Encoder[T] = Encoders.product[T]
  val tt: TypeTag[T] = typeTag[T]

  def isProcessed(targetDate: LocalDate): Boolean = {
    val successFile = getSuccessFilePath(targetDate)
    if (FSUtils.fileExists(successFile)(spark)) {
      return true
    }
    false
  }

  def getSuccessFilePath(targetDate: LocalDate): String = {
    val outputPath = this.datePartitionedPath(partition = Some(targetDate))
    s"$outputPath/_SUCCESS"
  }

}