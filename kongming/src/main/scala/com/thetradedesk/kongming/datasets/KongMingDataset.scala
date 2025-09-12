package com.thetradedesk.kongming.datasets

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming.{BaseFolderPath, EnableMetastore, EnablePartitionRegister, JobExperimentName, MLPlatformS3Root, getExperimentVersion, schemaPolicy, task, writeThroughHdfs}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet.buildPath
import com.thetradedesk.spark.datasets.core.SchemaPolicy.StrictCaseClassSchema
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.util.{ProdTesting, Production, Testing}
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

/**
 * This class serves as the basis for all date partitioned data sets in kongming for ease of reusability
 */
abstract class KongMingDataset[T <: Product : Manifest](dataSetType: DataSetType = GeneratedDataSet,
                                                        s3DatasetPath: String,
                                                        fileFormat: FileFormat = Parquet,
                                                        partitionField: String = "date",
                                                        experimentOverride: Option[String] = None)(implicit sparkSession: SparkSession = TTDSparkContext.spark)
  extends DatePartitionedS3DataSet[T](
    dataSetType = dataSetType,
    s3RootPath = MLPlatformS3Root,
    rootFolderPath = s"${BaseFolderPath}/${s3DatasetPath}",
    fileFormat = fileFormat,
    partitionField = partitionField,
    writeThroughHdfs = writeThroughHdfs,
    experimentOverride = experimentOverride,
    schemaPolicy = schemaPolicy
  )
  with MetastoreHandler {

  override val datasetTypeMh: DataSetType = dataSetType
  override val isInChainMh: Boolean = isInChain
  override val isExperimentMh: Boolean = isExperiment
  override val supportMetastore: Boolean = isMetastoreCompatibleDataFormat
  override val tableName: String = getMetastoreTableName
  override val metastorePartitionField1: String = "date"
  override val isStrictCaseClassSchema: Boolean = finalSchemaPolicy == StrictCaseClassSchema

  protected def getMetastoreTableName: String = {
    "unknown_table"
  }

  protected def isMetastoreCompatibleDataFormat: Boolean = true

  protected def registerMetastorePartition(partition: LocalDate): Unit = {
    registerPartitionToMetastore(partition)
  }

  protected def writeMetastorePartition(dataSet: Dataset[T], partition1: LocalDate, coalesceToNumFiles: Option[Int]): Unit = {
    writePartitionToMetastore(dataSet, partition1, coalesceToNumFiles)
  }

  def writePartition(dataSet: Dataset[T], partition: LocalDate, coalesceToNumFiles: Option[Int]): (String, Long) = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }

    var count: Long = 0L

    if (shouldUseMetastoreForReadAndWrite()) {
      val listener = new WriteListener()
      spark.sparkContext.addSparkListener(listener)

      writeMetastorePartition(dataSet, partition, coalesceToNumFiles)

      count = listener.rowsWritten

      spark.sparkContext.removeSparkListener(listener)
    }
    else {
      // write partition to S3 folders
      count = super.writePartition(dataSet, partition, coalesceToNumFiles)

      // register the partition
      if(shouldUseMetastoreForPartitionRegister()) {
        registerMetastorePartition(partition)
      }

      println(s"[LOG] Partition not written with Metastore")
    }
    // recount the rows in partition if writePartition returns 0 and the partition actually exists
    if (count == 0 && partitionExists(partition)) {
      count = readPartition(partition).count()
    }
    // set environment back to ProdTesting after getting the dataset count if the environment is actually ProdTesting
    if (isProdTesting) {
      environment = ProdTesting
    }
    // save row count as metadata
    val dataSetName = this.getClass.getSimpleName
    MetadataDataset(getExperimentVersion).writeRecord(count, partition, MetadataType.rowCount, dataSetName)

    (dataSetName, count)

  }

  override def partitionExists(partition: LocalDate): Boolean = {
    val path = buildPath(
      rootFolderPath,
      partitionField -> partition.format(dateTimeFormat)
    )
    FSUtils.directoryExists(s"$readRoot$path")
  }

  override def readRange(fromDate: LocalDateTime, toDate: LocalDateTime, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    println(s"[readRange] Invoked with from=$fromDate, to=$toDate")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readRangeFromMetastore(fromDate, toDate, isInclusive, verbose), schema)
    }
    else {
      super.readRange(fromDate, toDate, isInclusive, verbose=true)
    }
  }

  override def readDate(dayToRead: String): Dataset[T] = {

    println(s"[readDate] Read Date of $dayToRead")

    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readDateFromMetastore(dayToRead), schema)
    }
    else {
      super.readDate(dayToRead)
    }
  }

  override def readBackFromDate(
                        fromDate: LocalDate,
                        numberOfDaysToRead: Int,
                        maxExtraCheckDays: Int,
                        isInclusive: Boolean = true,
                        verbose: Boolean = false
                      ): Dataset[T] = {
    println(s"[readBackFromDate] fromDate=$fromDate, numberOfDaysToRead=$numberOfDaysToRead, maxExtraCheckDays=$maxExtraCheckDays, isInclusive=$isInclusive")

    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readBackFromDateFromMetastore(fromDate, numberOfDaysToRead, maxExtraCheckDays, isInclusive), schema)
    }
    else {
      super.readBackFromDate(fromDate, numberOfDaysToRead, maxExtraCheckDays, isInclusive, verbose = true)
    }
  }

  override def readPartition(value: String)(implicit i1: DummyImplicit): Dataset[T] = {
    println(s"[readPartition] Read partition of $value")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readPartitionFromMetastore(value), schema)
    }
    else {
      super.readPartition(value)
    }
  }

  override def readLatestPartition(verbose: Boolean = true): Dataset[T] = {
    println(s"[readLatestPartition] ...")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readLatestPartitionFromMetastore(), schema)
    }
    else {
      super.readLatestPartition(verbose = true)
    }
  }

  override def readLatestPartitionUpTo(
                                        maxInclusivePartition: LocalDate,
                                        isInclusive: Boolean = false,
                                        verbose: Boolean = false
                                      ): Dataset[T] = {
    println(s"[readLatestPartitionUpTo] ...")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readLatestPartitionUpToFromMetastore(maxInclusivePartition, isInclusive), schema)
    }
    else {
      super.readLatestPartitionUpTo(maxInclusivePartition, isInclusive, verbose = true)
    }
  }
}
