package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.generators.Feature
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class CBufferScanBuilder(sparkSession: SparkSession,
                              fileIndex: PartitioningAwareFileIndex,
                              schema: StructType,
                              dataSchema: StructType,
                              options: CaseInsensitiveStringMap
                             )
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters {

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var _pushedFilters: Array[Filter] = Array.empty

  // todo support it
  override def pushFilters(filters: Array[Filter]): Array[Filter] = filters

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): CBufferScan = {
    CBufferScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
      readPartitionSchema(), pushedFilters() ,options)
  }
}
