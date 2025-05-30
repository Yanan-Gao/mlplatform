package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.configs.UserFeatureMergeDefinition
import org.apache.spark.sql.{Encoder, Encoders}
import scala.reflect.runtime.universe._

class CustomBufferDataset[T <: Product: Manifest](
                                      userFeatureMergeDefinition: UserFeatureMergeDefinition
                                    )
  extends LightReadableDataset[T] with LightWritableDataset[T] {
  val rootPath: String = userFeatureMergeDefinition.rootPath
  val dataSetPath: String = userFeatureMergeDefinition.dataSetPath
  val defaultNumPartitions: Int = userFeatureMergeDefinition.config.defaultNumPartitions
  override val maxRecordsPerFile: Int = userFeatureMergeDefinition.config.maxRecordsPerFile
  override val repartitionColumn: Option[String] = Some(userFeatureMergeDefinition.sourceIdKey)
  val enc: Encoder[T] = Encoders.product[T]
  val tt: TypeTag[T] = typeTag[T]
  override val dateFormat: String = UserFeatureMergeDefinition.dateFormat
  override val writeThroughHdfs: Boolean = true
}
