package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.constants.FeatureConstants._
import com.thetradedesk.featurestore.entities.Result
import com.thetradedesk.featurestore.ttdEnv
import com.thetradedesk.featurestore.utils.PathUtils
import upickle.default._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class UserFeatureMergeDefinition(
                                       name: String,
                                       rootPath: String = FeatureConstants.ML_PLATFORM_S3_PATH,
                                       featureSourceDefinitions: Array[FeatureSourceDefinition],
                                       format: String = "parquet",
                                       config: UserFeatureMergeConfiguration = UserFeatureMergeConfiguration()
                                     ) {
  lazy val validate: Result = {
    if (name.isEmpty || !name.matches(AlphaNumericRegex)) {
      Result.failed("user feature merge definition name is invalid")
    } else if (featureSourceDefinitions.isEmpty ||
      config == null) {
      Result.failed(s"user feature merge definition ${name} is invalid")
    } else if (featureSourceDefinitions.groupBy(_.name).exists(_._2.length > 1)) {
      Result.failed(s"feature source must be distinct")
    } else if (!config.validate.success) {
      config.validate
    } else {
      featureSourceDefinitions.collectFirst({ case f if !f.validate.success => f.validate }).getOrElse(Result.succeed())
    }
  }

  lazy val dataSetPath: String = s"features/feature_store/${ttdEnv}/user_features_merged/v=1"
  private lazy val dataMetaPath: String = s"features/feature_store/${ttdEnv}/user_features_merged_meta/v=1"

  private def metaPath(dateTime: LocalDateTime): String = PathUtils.concatPath(rootPath, PathUtils.concatPath(dataMetaPath, UserFeatureMergeDefinition.dateFormatter.format(dateTime)))
  def schemaPath(dateTime: LocalDateTime): String = PathUtils.concatPath(metaPath(dateTime), SchemaFileName)
}

case class UserFeatureMergeConfiguration(
                                          maxDataSizePerRecord: Int = DefaultMaxDataSizePerRecord,
                                          defaultNumPartitions: Int = UserFeatureDataPartitionNumbers,
                                          maxRecordsPerFile: Int = DefaultMaxRecordsPerFile
                                        ) {
  lazy val validate: Result = {
    if (maxDataSizePerRecord <= 0) {
      Result.failed("data size per record can't be less than 0")
    } else if (defaultNumPartitions <=  0) {
      Result.failed("default number of partitions can't be less than 0")
    } else {
      Result.succeed()
    }
  }
}

object UserFeatureMergeDefinition {
  implicit val userFeatureMergeDefinitionRW: ReadWriter[UserFeatureMergeDefinition] = macroRW[UserFeatureMergeDefinition]

  val dateFormat = "yyyyMMddHH"
  val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
}

object UserFeatureMergeConfiguration {
  implicit val userFeatureMergeConfigurationRW: ReadWriter[UserFeatureMergeConfiguration] = macroRW[UserFeatureMergeConfiguration]
}