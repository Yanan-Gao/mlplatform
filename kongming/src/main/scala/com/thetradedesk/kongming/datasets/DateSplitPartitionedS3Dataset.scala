package com.thetradedesk.kongming.datasets

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming.{EnableMetastoreWrite, EnablePartitionRegister, JobExperimentName, getExperimentVersion, task, writeThroughHdfs, MetastoreTempViewName}
import com.thetradedesk.spark.datasets.core._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, max, min, size}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
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
)
  extends PartitionedS3DataSet2[T, LocalDate, String, String, String](
    dataSetType, s3RootPath, rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "split" -> ColumnExistsInDataSet,
    fileFormat,
    writeThroughHdfs = writeThroughHdfs,
    schemaPolicy = schemaPolicy,
    experimentOverride = experimentOverride
  ) {

  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet

  def partitionField2: (String, PartitionColumnCalculation) = "split" -> ColumnExistsInDataSet

  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  override def toStoragePartition1(date: LocalDate): String = date.format(dateTimeFormat)

  override def toStoragePartition2(split: String): String = split

  protected def getMetastoreTableName: String = {
    "unknown_table"
  }

  protected def getMetastoreDbName: String = {
    (task, environment) match {
      case ("roas", Production) => "roas"
      case ("roas", _) => "roas_test"

      case (_, Production) => "cpa"
      case (_, _) => "cpa_test"
    }
  }

  protected def supportsMetastorePartition: Boolean = true

  protected def registerMetastorePartition(partition1: LocalDate, partition2: String): Unit = {
    if (!supportsMetastorePartition || isExperiment) return

    val db = getMetastoreDbName
    val table = getMetastoreTableName
    val datePart = partition1.format(DateTimeFormatter.BASIC_ISO_DATE)

    try {
      if (table == "unknown_table") {
        throw new IllegalStateException("Subclasses must override `getMetastoreTableName` to provide a valid table name.")
      }

      val sqlStatement =
        s"""ALTER TABLE `$db`.`$table`
           |ADD IF NOT EXISTS PARTITION (date='$datePart', split='$partition2')""".stripMargin

      println(sqlStatement)
      spark.sql(sqlStatement)
      spark.catalog.refreshTable(s"$db.$table")

    } catch {
      case e: IllegalStateException =>
        println(s"[ERROR] Partition registration skipped due to missing metastore table name: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"[ERROR] Metastore operation failed for table `$db`.`$table`: ${e.getMessage}")
      case e: Exception =>
        println(s"[ERROR] Unexpected error during partition registration: ${e.getMessage}")
    }
  }

  protected def writeToMetastore(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): Unit = {
    val dbName = getMetastoreDbName
    val tableName = getMetastoreTableName

    val reshapedData = coalesceToNumFiles match {
      case Some(n) if n > 0 =>
        val repartitioned = dataSet.repartition(n)
        println(s"Repartitioned dataset to $n partitions.")
        repartitioned
      case _ =>
        println(s"No coalesce applied.")
        dataSet
    }

    reshapedData.createOrReplaceTempView(MetastoreTempViewName)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $dbName.$tableName
         |PARTITION (date='${partition1.format(DateTimeFormatter.BASIC_ISO_DATE)}', split='${partition2}')
         |SELECT * FROM $MetastoreTempViewName
         |""".stripMargin)

    println(s"Writing to partition: date='${partition1.format(DateTimeFormatter.BASIC_ISO_DATE)}', split='${partition2}' in table $dbName.$tableName")
  }

  def writePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): (String, Long) = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }

    var count: Long = 0L

    if (EnableMetastoreWrite && supportsMetastorePartition && !isExperiment) {
      val listener = new WriteListener()
      spark.sparkContext.addSparkListener(listener)

      writeToMetastore(dataSet, partition1, partition2, coalesceToNumFiles)

      count = listener.rowsWritten

      spark.sparkContext.removeSparkListener(listener)
    }
    else {
      // write partition to S3 folders
      count = super.writePartition(dataSet, partition1, partition2, coalesceToNumFiles)

      // firstly register the partition
      if(EnablePartitionRegister) {
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
    val datePartitionedDataSet = dataSet
      .withColumn(partitionField1._1, lit(toStoragePartition1(date)))
      .as[T]

    write(datePartitionedDataSet, coalesceToNumFiles, overrideSourceWrite, cleanDestDir, fileNameSuffix)
  }

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
