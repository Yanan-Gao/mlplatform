package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.constants.FeatureConstants.UserIDKey
import com.thetradedesk.featurestore.data.rules.DataValidationRule
import com.thetradedesk.featurestore.entities.Result
import com.thetradedesk.featurestore.ttdEnv
import com.thetradedesk.featurestore.utils.PathUtils
import upickle.default._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class FeatureSourceDefinition(
                                    name: String,
                                    dataSetPath: String,
                                    rootPath: String = FeatureConstants.ML_PLATFORM_S3_PATH,
                                    features: Array[FeatureDefinition],
                                    idKey: String = UserIDKey,
                                    lookBack: Int = 0,
                                    lookBackOnDay: Boolean = true,
                                    format: String = "parquet",
                                    dataValidationRule: Option[DataValidationRule] = None,
                                    dataSource: DataSource = DataSource.DailyJob
                                  ) {
  lazy val validate: Result = {
    if (name.isEmpty) {
      Result.failed("feature source name can't be empty")
    } else if (idKey.isEmpty ||
      rootPath.isEmpty ||
      dataSetPath.isEmpty ||
      features.isEmpty) {
      Result.failed(s"feature source ${name} definition is invalid")
    } else if (features.groupBy(_.name).exists(_._2.length > 1)) {
      Result.failed(s"feature name of each feature source must be distinct")
    } else {
      features.collectFirst({ case f if !f.validate.success => f.validate }).getOrElse(Result.succeed())
    }
  }

  def basePath(dateTime: LocalDateTime): String = PathUtils.concatPath(PathUtils.concatPath(rootPath, dataSetPath), DataSource.basePath(this.dataSource, dateTime))
}

object FeatureSourceDefinition {
  implicit val featureSourceDefinitionRW: ReadWriter[FeatureSourceDefinition] = macroRW[FeatureSourceDefinition]
}