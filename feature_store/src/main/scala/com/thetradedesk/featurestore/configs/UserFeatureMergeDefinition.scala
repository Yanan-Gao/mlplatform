package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants._
import com.thetradedesk.featurestore.entities.Result
import com.thetradedesk.featurestore.utils.PathUtils
import upickle.default._

case class UserFeatureMergeDefinition(
                                       name: String,
                                       dataSetPath: String,
                                       rootPath: String,
                                       featureSourceDefinitions: Array[FeatureSourceDefinition],
                                       format: String = "parquet",
                                       config: UserFeatureMergeConfiguration = UserFeatureMergeConfiguration()
                                     ) {
  lazy val validate: Result = {
    if (name.isEmpty || !name.matches(AlphaNumericRegex)) {
      Result.failed("user feature merge definition name is invalid")
    } else if (rootPath.isEmpty ||
      dataSetPath.isEmpty ||
      featureSourceDefinitions.isEmpty ||
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

  lazy val basePath: String = PathUtils.concatPath(rootPath, dataSetPath)
  lazy val metaPath: String = PathUtils.concatPath(basePath, MetaFolder)
  lazy val schemaPath: String = PathUtils.concatPath(metaPath, "schema.json")
}

case class UserFeatureMergeConfiguration(
                                          maxDataSizePerRecord: Int = DefaultMaxDataSizePerRecord,
                                          defaultNumPartitions: Int = UserFeatureDataPartitionNumbers
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
}

object UserFeatureMergeConfiguration {
  implicit val userFeatureMergeConfigurationRW: ReadWriter[UserFeatureMergeConfiguration] = macroRW[UserFeatureMergeConfiguration]
}