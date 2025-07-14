package com.thetradedesk.kongming.datasets


import com.thetradedesk.spark.datasets.core._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{DefaultUseFirstFileSchema, SchemaPolicyType}
import com.thetradedesk.kongming.MetastoreTempViewName

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ExtendedDateSplitS3Dataset[T <: Product : Manifest]
(
  dataSetType: DataSetType,
  s3RootPath: String,
  rootFolderPath: String,
  fileFormat: FileFormat,
  val extraPartitionName: String,
  val extraPartitionValue: String,
  schemaPolicy: SchemaPolicyType = DefaultUseFirstFileSchema,
  experimentOverride: Option[String] = None
)
  extends DateSplitPartitionedS3Dataset[T](
    dataSetType,
    s3RootPath,
    s"$rootFolderPath/$extraPartitionName=$extraPartitionValue/",
    fileFormat,
    schemaPolicy,
    experimentOverride
  ) {

  override protected def writeToMetastore(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): Unit = {
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
         |PARTITION (
         |  ${extraPartitionName}='${extraPartitionValue}',
         |  date='${partition1.format(DateTimeFormatter.BASIC_ISO_DATE)}',
         |  split='${partition2}'
         |)
         |SELECT * FROM $MetastoreTempViewName
         |""".stripMargin)

    println(s"Writing to partition: ${extraPartitionName}='${extraPartitionValue}', date='${partition1.format(DateTimeFormatter.BASIC_ISO_DATE)}', split='${partition2}' in table $dbName.$tableName")
  }

  override def writePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): (String, Long) = {
    val (dataSetName, count) = super.writePartition(dataSet, partition1, partition2, coalesceToNumFiles)

    (dataSetName, count)
  }

  override protected def registerMetastorePartition(date: LocalDate, split: String): Unit = {
    if (!supportsMetastorePartition || isExperiment) return

    val db = getMetastoreDbName
    val table = getMetastoreTableName
    val datePart = date.format(DateTimeFormatter.BASIC_ISO_DATE)

    try {
      if (table == "unknown_table") {
        throw new IllegalStateException("Subclasses must override `getMetastoreTableName`.")
      }

      val sqlStatement =
        s"""ALTER TABLE `$db`.`$table`
           |ADD IF NOT EXISTS PARTITION ($extraPartitionName='$extraPartitionValue', date='$datePart', split='$split')""".stripMargin

      println(sqlStatement)
      spark.sql(sqlStatement)
      spark.catalog.refreshTable(s"$db.$table")

    } catch {
      case e: IllegalStateException =>
        println(s"[ERROR] Partition registration skipped: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"[ERROR] Metastore operation failed: ${e.getMessage}")
      case e: Exception =>
        println(s"[ERROR] Unexpected error during partition registration: ${e.getMessage}")
    }
  }

}

