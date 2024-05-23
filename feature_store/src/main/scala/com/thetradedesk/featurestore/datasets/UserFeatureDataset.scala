package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.configs.UserFeatureMergeDefinition
import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.constants.FeatureConstants.UserFeatureDataPartitionNumbers
import com.thetradedesk.featurestore.ttdEnv
import com.thetradedesk.featurestore.utils.PathUtils
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Encoder, Encoders}

import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

case class UserFeature(TDID: String,
                       data: Array[Byte]
                      )

case class UserFeatureDataset(
                               userFeatureMergeDefinition: UserFeatureMergeDefinition
                             )
  extends LightReadableDataset[UserFeature] with LightWritableDataset[UserFeature] {
  val rootPath: String = userFeatureMergeDefinition.rootPath
  val dataSetPath: String = userFeatureMergeDefinition.dataSetPath
  val defaultNumPartitions: Int = userFeatureMergeDefinition.config.defaultNumPartitions
  override val maxRecordsPerFile: Int = userFeatureMergeDefinition.config.maxRecordsPerFile
  override val repartitionColumn: Option[String] = Some(FeatureConstants.UserIDKey)
  val enc: Encoder[UserFeature] = Encoders.product[UserFeature]
  val tt: TypeTag[UserFeature] = typeTag[UserFeature]
  override val dateFormat: String = UserFeatureMergeDefinition.dateFormat
}