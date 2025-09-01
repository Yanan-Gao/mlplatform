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
                                       config: UserFeatureMergeConfiguration = UserFeatureMergeConfiguration(),
                                       datasetPrefix: String = "features/feature_store",
                                       datasetName: String = "user_features_merged",
                                       datasetVersion: Int = 1
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

  lazy val dataSetPath: String = s"${datasetPrefix}/${ttdEnv}/${datasetName}/v=${datasetVersion}"
  private lazy val dataMetaPath: String = s"${datasetPrefix}/${ttdEnv}/${datasetName}_meta/v=${datasetVersion}"
  lazy val dataMetaSchemaPath: String = s"${datasetPrefix}/${ttdEnv}/${datasetName}_meta/v=${datasetVersion}/_CURRENT"
  def versionStr(dateTime: LocalDateTime): String = UserFeatureMergeDefinition.dateFormatter.format(dateTime)

  private def metaPath(dateTime: LocalDateTime): String = PathUtils.concatPath(rootPath, PathUtils.concatPath(dataMetaPath, versionStr(dateTime)))
  def schemaPath(dateTime: LocalDateTime): String = PathUtils.concatPath(metaPath(dateTime), SchemaFileName)

  lazy val sourceIdKey = featureSourceDefinitions.map(e => e.idKey).head
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