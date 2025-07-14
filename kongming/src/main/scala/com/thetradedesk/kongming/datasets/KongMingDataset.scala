package com.thetradedesk.kongming.datasets

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming.{BaseFolderPath, EnableMetastoreWrite, EnablePartitionRegister, JobExperimentName, MLPlatformS3Root, MetastoreTempViewName, getExperimentVersion, schemaPolicy, task, writeThroughHdfs}
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet.buildPath
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.util.{ProdTesting, Production, Testing}
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.lit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * This class serves as the basis for all date partitioned data sets in kongming for ease of reusability
 */
abstract class KongMingDataset[T <: Product : Manifest](dataSetType: DataSetType = GeneratedDataSet,
                                                        s3DatasetPath: String,
                                                        fileFormat: FileFormat = Parquet,
                                                        partitionField: String = "date",
                                                        experimentOverride: Option[String] = None)
  extends DatePartitionedS3DataSet[T](
    dataSetType = dataSetType,
    s3RootPath = MLPlatformS3Root,
    rootFolderPath = s"${BaseFolderPath}/${s3DatasetPath}",
    fileFormat = fileFormat,
    partitionField = partitionField,
    writeThroughHdfs = writeThroughHdfs,
    experimentOverride = experimentOverride,
    schemaPolicy = schemaPolicy
  ) {

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

  protected def registerMetastorePartition(partition: LocalDate): Unit = {
    val partValue = partition.format(DateTimeFormatter.BASIC_ISO_DATE)

    if (supportsMetastorePartition && !isExperiment) {
      val db = getMetastoreDbName
      val table = getMetastoreTableName
      try {
        if (table == "unknown_table") {
          throw new IllegalStateException(
            "Subclasses must override `getMetastoreTableName` to provide a valid table name."
          )
        }

        val sqlStatement =
          s"""ALTER TABLE `$db`.`$table`
             |ADD IF NOT EXISTS PARTITION ($partitionField='$partValue')""".stripMargin

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
  }

  protected def supportsMetastorePartition: Boolean = true

  private def writeToMetastore(dataSet: Dataset[T], partition: LocalDate, coalesceToNumFiles: Option[Int]): Unit = {
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
         |PARTITION (date='${partition.format(DateTimeFormatter.BASIC_ISO_DATE)}')
         |SELECT * FROM $MetastoreTempViewName
         |""".stripMargin)

    println(s"Writing to partition: date='${partition.format(DateTimeFormatter.BASIC_ISO_DATE)}' in table $dbName.$tableName")
  }

  def writePartition(dataSet: Dataset[T], partition: LocalDate, coalesceToNumFiles: Option[Int]): (String, Long) = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }

    var count: Long = 0L

    if (EnableMetastoreWrite && supportsMetastorePartition && !isExperiment) {
      val listener = new WriteListener()
      spark.sparkContext.addSparkListener(listener)

      writeToMetastore(dataSet, partition, coalesceToNumFiles)

      count = listener.rowsWritten

      spark.sparkContext.removeSparkListener(listener)
    }
    else {
      // write partition to S3 folders
      count = super.writePartition(dataSet, partition, coalesceToNumFiles)

      // register the partition
      if(EnablePartitionRegister) {
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

}
