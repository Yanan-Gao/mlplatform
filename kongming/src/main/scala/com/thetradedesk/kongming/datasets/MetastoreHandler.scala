package com.thetradedesk.kongming.datasets
import com.thetradedesk.kongming.{EnableMetastore, EnablePartitionRegister, task}
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.util.{ProdTesting, Production, Testing}
import com.thetradedesk.spark.util.TTDConfig.environment
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.types.StructType

import java.time.{LocalDate, LocalDateTime}
import scala.reflect.runtime.universe.TypeTag
import java.time.format.DateTimeFormatter

trait MetastoreHandler {
  val datasetTypeMh: DataSetType
  val isInChainMh: Boolean
  val isExperimentMh: Boolean
  val supportMetastore: Boolean
  val tableName: String
  val metastorePartitionField1: String
  val metastorePartitionField2: String = "unused"
  val metastorePartitionField3: String = "unused"
  val isStrictCaseClassSchema: Boolean

  val CPA: String = "cpa"
  val CPA_TEST: String = "cpa_test"
  val ROAS: String = "roas"
  val ROAS_TEST: String = "roas_test"

  def getDbNameForRead(datasetType: DataSetType = datasetTypeMh, isInChain: Boolean = isInChainMh, task: String = task): String = {
    (environment, datasetType, isInChain, task) match {
      case (Production, _, _, "cpa") => CPA
      case (Production, _, _, "roas") => ROAS
      case (ProdTesting, _, false, "cpa") => CPA
      case (ProdTesting, _, false, "roas") => ROAS
      case (ProdTesting, _, true, "cpa") => CPA_TEST
      case (ProdTesting, _, true, "roas") => ROAS_TEST
      case (Testing, SourceDataSet, _, "cpa") => CPA
      case (Testing, SourceDataSet, _, "roas") => ROAS
      case (Testing, GeneratedDataSet, _, "cpa") => CPA_TEST
      case (Testing, GeneratedDataSet, _, "roas") => ROAS_TEST
      case (_, _, _, unknownTask) =>
        throw new IllegalArgumentException(s"Unknown task name: $unknownTask")
      case (unknownEnv, unknownDatasetType, unknownChain, _) =>
        throw new IllegalArgumentException(s"Unsupported combination: ($unknownEnv, $unknownDatasetType, $unknownChain)")
    }
  }

  def getDbNameForWrite(task: String = task): String = {
    (environment, task) match {
      case (Production, "cpa") => CPA
      case (ProdTesting, "cpa") => CPA_TEST
      case (Testing, "cpa") => CPA_TEST
      case (Production, "roas") => ROAS
      case (ProdTesting, "roas") => ROAS_TEST
      case (Testing, "roas") => ROAS_TEST
      case (_, unknownTask) =>
        throw new IllegalArgumentException(s"Unknown task name: $unknownTask")
      case (unknownEnv, _) =>
        throw new IllegalArgumentException(s"Unknown Environment: ($unknownEnv")
    }
  }

  /**
   * Determines whether to use Metastore for reading and writing data.
   *
   * The conditions for enabling Metastore usage are:
   *
   * 1. The `enableMetastore` flag is set to true, indicating that Metastore is enabled in the configuration;
   * 2. The dataset format supports Metastore:
   *    - Currently, only Parquet files are fully supported;
   *    - CSV files are not supported due to header handling issues when using Spark for I/O;
   *    - CBuffer format is not compatible with Metastore;
   * 3. The current environment is not an Experiment environment, where Metastore usage is explicitly disabled.
   *
   * Only when all three conditions are met will Metastore be used for data read/write operations.
   */

  def shouldUseMetastoreForReadAndWrite(): Boolean = EnableMetastore && supportMetastore && !isExperimentMh

  /**
   * Similar to `shouldUseMetastore`, but controlled by `enablePartitionRegister`.
   * This function only determines whether to register partitions in Metastore,
   * without affecting data read/write operations.
   */

  def shouldUseMetastoreForPartitionRegister(): Boolean = {
    EnablePartitionRegister && supportMetastore && !isExperimentMh
  }

  val dateTimeFormatString: String = DefaultTimeFormatStrings.dateTimeFormatString

  private def localDateTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatString)

  private def dateTimeFormat: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE

  def registerToMetastore(condition: String)(implicit spark: SparkSession): Unit = {
    val db = getDbNameForWrite()

    try {
      if (tableName == "unknown_table") {
        throw new IllegalStateException(
          "Subclasses must override `getMetastoreTableName` to provide a valid table name."
        )
      }

      val sql =
        s"""ALTER TABLE `$db`.`$tableName`
           |ADD IF NOT EXISTS PARTITION ($condition)""".stripMargin

      println(sql)
      spark.sql(sql)
      spark.catalog.refreshTable(s"$db.$tableName")

    } catch {
      case e: IllegalStateException =>
        println(s"[ERROR] Partition registration skipped due to missing metastore table name: ${e.getMessage}")
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"[ERROR] Metastore operation failed for table `$db`.`$tableName`: ${e.getMessage}")
      case e: Exception =>
        println(s"[ERROR] Unexpected error during partition registration: ${e.getMessage}")
    }
  }

  def registerPartitionToMetastore(partition1: LocalDate)(implicit spark: SparkSession): Unit = {
    val partition_1 = partition1.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition_1'"
    registerToMetastore(condition)
  }

  def registerPartitionToMetastore(partition1: LocalDate, partition2: String)(implicit spark: SparkSession): Unit = {
    val partition_1 = partition1.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition_1', $metastorePartitionField2 = '$partition2'"
    registerToMetastore(condition)
  }

  def registerPartitionToMetastore(partition1: String, partition2: LocalDate, partition3: String)(implicit spark: SparkSession): Unit = {
    val partition_2 = partition2.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition1', $metastorePartitionField2 = '$partition_2', $metastorePartitionField3 = '$partition3'"
    registerToMetastore(condition)
  }

  def writeToMetastore(reshapedData: Dataset[_], tempViewName: String, condition: String)(implicit spark: SparkSession): Unit = {
    val db = getDbNameForWrite()

    reshapedData.createOrReplaceTempView(tempViewName)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $db.$tableName
         |PARTITION ($condition)
         |SELECT * FROM $tempViewName
         |""".stripMargin)

    println(s"Writing to partition: $condition in table $db.$tableName")
  }

  def applyCoalesce[T](dataSet: Dataset[T], coalesceToNumFiles: Option[Int]): Dataset[T] = {
    coalesceToNumFiles match {
      case Some(n) if n > 0 =>
        val repartitioned = dataSet.repartition(n)
        println(s"Repartitioned dataset to $n partitions.")
        repartitioned
      case _ =>
        println(s"No coalesce applied.")
        dataSet
    }
  }

  def writePartitionToMetastore(dataSet: Dataset[_], partition1: LocalDate, coalesceToNumFiles: Option[Int])(implicit spark: SparkSession): Unit = {
    val reshapedData = applyCoalesce(dataSet, coalesceToNumFiles)

    val partition_1 = partition1.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition_1'"

    val dbName = getDbNameForWrite()
    val tempViewName = sanitizeString(s"${dbName}_${tableName}_${metastorePartitionField1}_${partition_1}")

    writeToMetastore(reshapedData, tempViewName, condition)
  }

  def writePartitionToMetastore(dataSet: Dataset[_], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int])(implicit spark: SparkSession): Unit = {
    val reshapedData = applyCoalesce(dataSet, coalesceToNumFiles)

    val partition_1 = partition1.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition_1', $metastorePartitionField2 = '$partition2'"

    val dbName = getDbNameForWrite()
    val tempViewName = sanitizeString(s"${dbName}_${tableName}_${metastorePartitionField1}_${partition_1}_${metastorePartitionField2}_${partition2}")

    writeToMetastore(reshapedData, tempViewName, condition)
  }

  def writePartitionToMetastore(dataSet: Dataset[_], partition1: String, partition2: LocalDate, partition3: String, coalesceToNumFiles: Option[Int])(implicit spark: SparkSession): Unit = {
    val reshapedData = applyCoalesce(dataSet, coalesceToNumFiles)

    val partition_2 = partition2.format(dateTimeFormat)
    val condition = s"$metastorePartitionField1 = '$partition1', $metastorePartitionField2 = '$partition_2', $metastorePartitionField3 = '$partition3'"

    val dbName = getDbNameForWrite()
    val tempViewName = sanitizeString(s"${dbName}_${tableName}_${metastorePartitionField1}_${partition1}_${metastorePartitionField2}_${partition_2}_${metastorePartitionField3}_${partition3}")

    writeToMetastore(reshapedData, tempViewName, condition)
  }


  def readFromMetastore(
                          filterCondition: String
                        )(implicit spark: SparkSession):  DataFrame = {
    val dbName = getDbNameForRead()
    val fullTableName = s"$dbName.$tableName"
    val sql = s"SELECT * FROM $fullTableName WHERE $filterCondition"
    println(s"Reading from Metastore via `$sql``")

    val rawDF = spark.sql(sql)

    // You can filter the csv headers by using this part
    /*
    if (rawDF.columns.contains("adgroupidstr")) {
      rawDF.filter(row => row.getAs[String]("adgroupidstr") != "AdGroupIdStr")
    }
    else rawDF
    */

    rawDF
  }

  def extractPartitionValues(
                              partitionsDF: DataFrame,
                              partitionField: String
                            ): Set[String] = {
    partitionsDF
      .collect()
      .map(_.getString(0))
      .flatMap { partition =>
        partition
          .split("/")
          .find(_.startsWith(s"$partitionField="))
          .map(_.split("=")(1))
      }
      .toSet
  }

  def getAvailablePartitionsFromMetastore(
                                           dbName: String,
                                           tableName: String,
                                           partitionField: String
                                         )(implicit spark: SparkSession): Set[String] = {
    val fullTableName = s"$dbName.$tableName"
    val partitionsDF = spark.sql(s"SHOW PARTITIONS $fullTableName")
    extractPartitionValues(partitionsDF, partitionField)
  }

  def filterAndExtractPartitionValues(
                                       partitions: Seq[String],
                                       partitionField: String,
                                       extraConditionField: String,
                                       extraConditionValue: String
                                     ): Set[String] = {
    partitions
      .filter { partitionStr =>
        partitionStr
          .split("/")
          .exists(_.trim == s"$extraConditionField=$extraConditionValue")
      }
      .flatMap { partitionStr =>
        partitionStr
          .split("/")
          .find(_.startsWith(s"$partitionField="))
          .map(_.split("=")(1))
      }
      .toSet
  }

  def getFilteredPartitionsFromMetastore(
                                          dbName: String,
                                          tableName: String,
                                          partitionField: String,
                                          extraConditionField: String,
                                          extraConditionValue: String
                                        )(implicit spark: SparkSession): Set[String] = {
    val fullTableName = s"$dbName.$tableName"
    val partitionsDF = spark.sql(s"SHOW PARTITIONS $fullTableName")
    val partitionStrs = partitionsDF.collect().map(_.getString(0))
    filterAndExtractPartitionValues(partitionStrs, partitionField, extraConditionField, extraConditionValue)
  }


  def readRangeFromMetastore(
                              fromDate: LocalDateTime,
                              toDate: LocalDateTime,
                              isInclusive: Boolean = false,
                              verbose: Boolean = false
                            )(implicit spark: SparkSession): DataFrame = {

    println(s"[readRangeFromMetastore] called with:")
    println(s"  fromDate    = $fromDate")
    println(s"  toDate      = $toDate")
    println(s"  isInclusive = $isInclusive")
    println(s"  verbose     = $verbose")

    if (fromDate.isAfter(toDate))
      throw new IllegalArgumentException(s"The fromDate $fromDate is later than the toDate $toDate")

    val fromStr = fromDate.format(localDateTimeFormat)
    val toStrRaw = toDate.format(localDateTimeFormat)
    val toStrFinal = if (isInclusive) toStrRaw else toDate.minusDays(1).format(localDateTimeFormat)

    val filterCondition = s"date BETWEEN '$fromStr' AND '$toStrFinal'"
    println(s"  filterCondition = $filterCondition")
    println(s"[readRangeFromMetastore] Executing readFromMetastore...")

    readFromMetastore(filterCondition)
  }

  def readDateFromMetastore(dayToRead: String)(implicit spark: SparkSession): DataFrame = {

    println(s"[readDateFromMetastore] called with:")
    println(s"  dayToRead     = $dayToRead")

    val filterCondition = s"date = '$dayToRead'"
    println(s"  filterCondition = $filterCondition")
    println(s"[readDateFromMetastore] Executing readFromMetastore...")

    val df = readFromMetastore(filterCondition)

    val rowCount = df.count()

    if (rowCount == 0) {
      throw new RuntimeException(s"No data found for '$filterCondition' via Metastore.")
    }

    df
  }

  def readBackFromDateFromMetastore(
                                     fromDate: LocalDate,
                                     numberOfDaysToRead: Int,
                                     maxExtraCheckDays: Int,
                                     isInclusive: Boolean = true
                                   )(implicit spark: SparkSession): DataFrame = {
    val firstDateToCheck =
      if (isInclusive) fromDate
      else fromDate.minusDays(1)

    val candidateDates =
      (0 until numberOfDaysToRead + maxExtraCheckDays)
        .map(offset => firstDateToCheck.minusDays(offset).format(localDateTimeFormat))

    val dbName = getDbNameForRead()
    val availableDates = getAvailablePartitionsFromMetastore(dbName, tableName, "date")

    val readableDates = candidateDates.filter(availableDates.contains).take(numberOfDaysToRead)

    if (readableDates.length < numberOfDaysToRead)
      throw new RuntimeException(
        s"Couldn't find enough readable partitions from ${firstDateToCheck} with maxExtraCheckDays = $maxExtraCheckDays."
      )

    val condition = s"date IN (${readableDates.map(date => s"'$date'").mkString(", ")})"

    readFromMetastore(condition)
  }


  def readPartitionFromMetastore(partition: String)(implicit spark: SparkSession): DataFrame = {

    println(s"[readPartitionFromMetastore] called with:")
    println(s"  partition     = $partition")

    val filterCondition = s"$metastorePartitionField1 = '$partition'"
    println(s"  filterCondition = $filterCondition")
    println(s"[readPartitionFromMetastore] Executing readFromMetastore...")

    val df = readFromMetastore(filterCondition)
    val rowCount = df.count()

    if (rowCount == 0) {
      throw new RuntimeException(s"No data found for '$filterCondition' via Metastore.")
    }

    df
  }

  def readPartitionFromMetastore(partition1: String, partition2: String)(implicit spark: SparkSession): DataFrame = {

    println(s"[readPartitionFromMetastore-2] called with:")
    println(s"  partition1 = $partition1")
    println(s"  partition2 = $partition2")

    val filterCondition =
      s"$metastorePartitionField1 = '$partition1' AND $metastorePartitionField2 = '$partition2'"

    println(s"  filterCondition = $filterCondition")
    println(s"[readPartitionFromMetastore-2] Executing readFromMetastore...")

    val df = readFromMetastore(filterCondition)
    val rowCount = df.count()

    if (rowCount == 0) {
      throw new RuntimeException(s"No data found for '$filterCondition' via Metastore.")
    }

    df
  }

  def readPartitionFromMetastore(partition1: String, partition2: String, partition3: String)(implicit spark: SparkSession): DataFrame = {

    println(s"[readPartitionFromMetastore-3] called with:")
    println(s"  partition1 = $partition1")
    println(s"  partition2 = $partition2")
    println(s"  partition3 = $partition3")

    val filterCondition =
      s"$metastorePartitionField1 = '$partition1' AND $metastorePartitionField2 = '$partition2' AND $metastorePartitionField3 = '$partition3'"

    println(s"  filterCondition = $filterCondition")
    println(s"[readPartitionFromMetastore-3] Executing readFromMetastore...")

    val df = readFromMetastore(filterCondition)
    val rowCount = df.count()

    if (rowCount == 0) {
      throw new RuntimeException(s"No data found for '$filterCondition' via Metastore.")
    }

    df
  }

  def readLatestPartitionFromMetastore()(implicit spark: SparkSession): DataFrame = {
    val dbName = getDbNameForRead()
    val availableDates = getAvailablePartitionsFromMetastore(dbName, tableName, "date")

    if (availableDates.isEmpty) {
      throw new RuntimeException(s"No available partitions found in $dbName.$tableName.")
    }

    val latestDate = availableDates.toSeq.max
    readPartitionFromMetastore(latestDate)
  }


  def readLatestPartitionFromMetastore(partition1: String)(implicit spark: SparkSession): DataFrame = {
    val dbName = getDbNameForRead()
    val availableDates = getFilteredPartitionsFromMetastore(dbName, tableName, "date", metastorePartitionField1, partition1)

    if (availableDates.isEmpty) {
      throw new RuntimeException(s"No available partitions found in $dbName.$tableName.")
    }

    val latestDate = availableDates.toSeq.max
    readPartitionFromMetastore(partition1, latestDate)
  }

  def readLatestPartitionUpToFromMetastore(
                               maxInclusivePartition: LocalDate,
                               isInclusive: Boolean = false
                             )(implicit spark: SparkSession): DataFrame = {
    val maxInclusivePartitionStr = maxInclusivePartition.format(localDateTimeFormat)

    val dbName = getDbNameForRead()
    val availableDates = getAvailablePartitionsFromMetastore(dbName, tableName, "date")

    val filteredDates = availableDates.filter { d =>
      if (isInclusive) d <= maxInclusivePartitionStr
      else d < maxInclusivePartitionStr
    }

    if (filteredDates.isEmpty) {
      throw new RuntimeException(
        s"No available partition found up to '$maxInclusivePartitionStr' with isInclusive = $isInclusive"
      )
    }

    val latestDate = filteredDates.max

    readPartitionFromMetastore(latestDate)
  }

  def getFieldNames(schema: StructType): Seq[String] = schema.fields.map(_.name)


  def buildRenameExprs(df: DataFrame, targetFields: Seq[String]): Seq[String] = {
    val dfFields = df.columns.map(_.toLowerCase)
    val renameExprs = targetFields.flatMap { field =>
      val lower = field.toLowerCase
      if (dfFields.contains(lower)) {
        Some(s"`$lower` as `$field`")
      } else {
        None
      }
    }

    renameExprs
  }

  def autoRenameAndAs[T: TypeTag : Encoder](df: DataFrame, schema: StructType): Dataset[T] = {
    val targetFields = getFieldNames(schema)
    val renameExprs = buildRenameExprs(df, targetFields)

    val renamedDF = df.selectExpr(renameExprs: _*)

    val tableCols = renamedDF.columns.toSet

    if (isStrictCaseClassSchema) {
      val expandedDF = schema.fields.foldLeft(renamedDF) { (df, field) =>
        if (!tableCols.contains(field.name)) {
          println(s"[autoRenameAndAs] Expanding missing column '${field.name}' as NULL of type ${field.dataType}")
          df.withColumn(field.name, typedLit(null).cast(field.dataType))
        } else df
      }
      expandedDF.as[T]
    }
    else renamedDF.as[T]
  }

  def sanitizeString(input: String): String = {
    input.replaceAll("[^A-Za-z0-9_]", "_")
  }
}
