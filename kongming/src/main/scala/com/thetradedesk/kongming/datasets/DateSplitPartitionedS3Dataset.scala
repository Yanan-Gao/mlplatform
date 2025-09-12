package com.thetradedesk.kongming.datasets

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming.{EnableMetastore, EnablePartitionRegister, JobExperimentName, getExperimentVersion, task, writeThroughHdfs}
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.datasets.core.SchemaPolicy.StrictCaseClassSchema
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, max, min, size}
import com.thetradedesk.spark.TTDSparkContext

import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.util.Environment
import com.thetradedesk.spark.util.{ProdTesting, Production, Testing}
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet.buildPath
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{DefaultUseFirstFileSchema, SchemaPolicyType}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

abstract class DateSplitPartitionedS3Dataset[T <: Product : Manifest]
(
  dataSetType: DataSetType,
  s3RootPath: String,
  rootFolderPath: String,
  fileFormat: FileFormat,
  schemaPolicy: SchemaPolicyType = DefaultUseFirstFileSchema,
  experimentOverride: Option[String] = None
)(implicit sparkSession: SparkSession = TTDSparkContext.spark)
  extends PartitionedS3DataSet2[T, LocalDate, String, String, String](
    dataSetType, s3RootPath, rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "split" -> ColumnExistsInDataSet,
    fileFormat,
    writeThroughHdfs = writeThroughHdfs,
    schemaPolicy = schemaPolicy,
    experimentOverride = experimentOverride
  )
    with MetastoreHandler {

  override val datasetTypeMh: DataSetType = dataSetType
  override val isInChainMh: Boolean = isInChain
  override val isExperimentMh: Boolean = isExperiment
  override val supportMetastore: Boolean = isMetastoreCompatibleDataFormat
  override val tableName: String = getMetastoreTableName
  override val metastorePartitionField1: String = "date"
  override val metastorePartitionField2: String = "split"
  override val isStrictCaseClassSchema: Boolean = finalSchemaPolicy == StrictCaseClassSchema

  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet

  def partitionField2: (String, PartitionColumnCalculation) = "split" -> ColumnExistsInDataSet

  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  override def toStoragePartition1(date: LocalDate): String = date.format(dateTimeFormat)

  override def toStoragePartition2(split: String): String = split

  protected def getMetastoreTableName: String = {
    "unknown_table"
  }

  protected def isMetastoreCompatibleDataFormat: Boolean = true

  protected def registerMetastorePartition(partition1: LocalDate, partition2: String): Unit = {
    registerPartitionToMetastore(partition1, partition2)
  }

  protected def writeMetastorePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): Unit = {
    writePartitionToMetastore(dataSet, partition1, partition2, coalesceToNumFiles)
  }

  def writePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): (String, Long) = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }

    var count: Long = 0L

    if (shouldUseMetastoreForReadAndWrite()) {
      val listener = new WriteListener()
      spark.sparkContext.addSparkListener(listener)

      writePartitionToMetastore(dataSet, partition1, partition2, coalesceToNumFiles)

      count = listener.rowsWritten

      spark.sparkContext.removeSparkListener(listener)
    }
    else {
      // write partition to S3 folders
      count = super.writePartition(dataSet, partition1, partition2, coalesceToNumFiles)

      // then register the partition
      if(shouldUseMetastoreForPartitionRegister()) {
        registerMetastorePartition(partition1, partition2)
      }

      println(s"[LOG] Partition not written with Metastore")
    }

    // recount the rows in partition if writePartition returns 0 and the partition actually exists
    if (count == 0 && partitionExists(partition1, partition2)) {
      count = readPartition(partition1, partition2).count()
    }
    // set environment back to ProdTesting after getting the dataset count if the environment is actually ProdTesting
    if (isProdTesting) {
      environment = ProdTesting
    }
    // save row count as metadata
    val dataSetName = s"${this.getClass.getSimpleName}/${partition2}"
    MetadataDataset(getExperimentVersion).writeRecord(count, partition1, MetadataType.rowCount, dataSetName)

    (dataSetName, count)

  }

  override def partitionExists(partition1: LocalDate, partition2: String): Boolean = {
    val path = buildPath(
      rootFolderPath,
      partitionField1._1 -> toStoragePartition1(partition1),
      partitionField2._1 -> toStoragePartition2(partition2)
    )
    FSUtils.directoryExists(s"$readRoot$path")
  }


  def writeDate(dataSet: Dataset[T],
                date: LocalDate,
                coalesceToNumFiles: Option[Int] = None,
                overrideSourceWrite: Boolean = false,
                cleanDestDir: Boolean = true,
                fileNameSuffix: Option[String] = None
               ): Long = {
    import spark.implicits._
    val datePartitionedDataSet = dataSet
      .withColumn(partitionField1._1, lit(toStoragePartition1(date)))
      .as[T]

    write(datePartitionedDataSet, coalesceToNumFiles, overrideSourceWrite, cleanDestDir, fileNameSuffix)
  }

  override def readStoragePartition(value: String): Dataset[T] = {
    println(s"[readStoragePartition1] Read partition of $value")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readPartitionFromMetastore(value), schema)
    }
    else {
      super.readStoragePartition(value)
    }
  }

  override def readStoragePartition(value1: String, value2: String): Dataset[T] = {
    println(s"[readPartition2] Read partition of $value1, $value2")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readPartitionFromMetastore(value1, value2), schema)
    }
    else {
      super.readStoragePartition(value1, value2)
    }
  }

  override def readLatestPartition(verbose: Boolean = true): Dataset[T] = {
    println(s"[readLatestPartition] ...")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readLatestPartitionFromMetastore(), schema)
    }
    else {
      super.readLatestPartition(verbose=true)
    }
  }

  def readDate(date: LocalDate): Dataset[T] =
    readPartition(date)
}

abstract class DateSplitPartitionedS3CBufferDataset[T <: Product : Manifest](
  s3RootPath: String,
  rootFolderPath: String,
  subFolderKey: Option[String] = None,
  experimentOverride: Option[String] = None
) {
  private def ConcatPath(l: String, r: String): String = {
    TrimPath(l) + "/" + TrimPath(r)
  }

  // remove slash letter with head and tail
  private def TrimPath(path: String): String = {
    path.stripPrefix("/").stripSuffix("/")
  }

  lazy val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet
  def partitionField2: (String, PartitionColumnCalculation) = subFolderKey.getOrElse("") -> ColumnExistsInDataSet

  def toStoragePartition1(date: LocalDate): String = date.format(dateFormatter)

  def DatePartitionedPath(env: Environment, date: LocalDate, subFolderKey: Option[String] = None, subFolderValue: Option[String] = None): String = {
    val envStr = if (environment == Production) "prod" else "test"
    var path = ConcatPath(s3RootPath, envStr)

    val experiment = config.getString(s"ttd.${this.getClass.getSimpleName}.experiment", "")
    if (experiment != "") {
      path = ConcatPath(path, s"experiment=$experiment")
    }

    path = ConcatPath(path, rootFolderPath)
    path = ConcatPath(path, s"date=${date.format(dateFormatter)}")

    if (subFolderKey.isDefined) {
      path = ConcatPath(path, s"${subFolderKey.getOrElse("")}=${subFolderValue.getOrElse("")}")
    }

    path
  }

  def writePartition(
    dataSet: Dataset[T], partition1: LocalDate, partition2: Option[String],
    coalesceToNumFiles: Int, maxChunkRecordCount: Int, saveMode: SaveMode = SaveMode.Overwrite
  )(implicit spark: SparkSession = TTDSparkContext.spark): (String, Long) = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }

    val partitionedPath: String = DatePartitionedPath(environment, partition1, subFolderKey, partition2)

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    println("Writing CBuffer " + partitionedPath)
    dataSet
      .repartition(coalesceToNumFiles)
      .write.mode(saveMode)
      .format("com.thetradedesk.featurestore.data.cbuffer.CBufferDataSource")
      .option("maxChunkRecordCount", maxChunkRecordCount)
      .save(partitionedPath)

    val count = listener.rowsWritten

    spark.sparkContext.removeSparkListener(listener)

    // set environment back to ProdTesting after getting the dataset count if the environment is actually ProdTesting
    if (isProdTesting) {
      environment = ProdTesting
    }
    // save row count as metadata
    val dataSetName = s"${this.getClass.getSimpleName}/${partition2}"
    MetadataDataset(getExperimentVersion).writeRecord(count, partition1, MetadataType.rowCount, dataSetName)

    (dataSetName, count)
  }

}
