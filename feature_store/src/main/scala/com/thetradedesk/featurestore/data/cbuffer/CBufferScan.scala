package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

case class CBufferScan(
                        sparkSession: SparkSession,
                        hadoopConf: Configuration,
                        fileIndex: PartitioningAwareFileIndex,
                        dataSchema: StructType,
                        readDataSchema: StructType,
                        readPartitionSchema: StructType,
                        pushedFilters: Array[Filter],
                        options: CaseInsensitiveStringMap,
                        partitionFilters: Seq[Expression] = Seq.empty,
                        dataFilters: Seq[Expression] = Seq.empty) extends FileScan {

  override def createReaderFactory(): PartitionReaderFactory = {
    val sqlConf = sparkSession.sessionState.conf
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    val parsedOptions = CBufferOptions(options.asScala.toMap)

    CBufferPartitionReaderFactory(sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      parsedOptions,
      pushedFilters
    )
  }

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }
}
