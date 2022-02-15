package com.ttd.contextual.datasets.sources

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.regex.Pattern

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{S3Roots, SourceDataSet}
import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import com.ttd.contextual.spark.TTDSparkContext
import com.ttd.contextual.spark.TTDSparkContext.spark
import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import com.ttd.contextual.util.elDoradoUtilities.datasets.core._
import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.lit

/**
 * Most methods from DateHourPartitionedS3DataSet have been overwritten in this class
 * This is because the super class does not handle cases when the dateField and hourField names are empty
 */
case class IdentityAvailsOld(mergeSchema: Boolean = false)
  extends DateHourPartitionedS3DataSet[IdentityAvailsRecord](
    SourceDataSet,
    S3Roots.IDENTITY_ROOT,
    "sources/avails-idnt",
    dateField = "date" -> ColumnExistsInDataSet,
    hourField = "hour" -> ColumnExistsInDataSet,
    formatStrings = DateHourFormatStrings(
      "yyyy-MM-dd",
      "HH"
    ),
    mergeSchema = mergeSchema
  ) {
  override val dateHourFolderPathRegex: Pattern = Pattern.compile(s"/(.*?)/(.*?)(?:/|$$)")
  override def readDate(date: String): Dataset[IdentityAvailsRecord] = {
    val dateHours = (0 until 24) map (x => "%02d".format(x)) map (hour => s"$rootFolderPath/$date/$hour")
    read(dateHours:_*)
  }

  override def readHour(date: String, hour: Int): Dataset[IdentityAvailsRecord] = {
    val hourString = "%02d".format(hour)
    read(s"$rootFolderPath/$date/$hourString")
  }

  override def readRange(fromTime: LocalDateTime, toTime: LocalDateTime, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[IdentityAvailsRecord] = {
    // spark does not play nice with archived files in s3. For this reason we need
    // to get and filter a list of files yourself, then pass it into spark.
    val hoursToIterate =
    if (isInclusive) fromTime.until(toTime, ChronoUnit.HOURS)
    else Math.max(fromTime.until(toTime, ChronoUnit.HOURS) - 1, 0)

    val foldersToRead = (0 to hoursToIterate.toInt)
      .map { currentHour =>
        val time = fromTime.plusHours(currentHour)
        s"$readRoot/$rootFolderPath/${time.format(dateTimeFormat)}/${time.format(hourTimeFormat)}"
      }
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
      .map(_.replace(s"$readRoot/", "")) // read only needs the path past the root, so remove anything before

    if (verbose) {
      println("Reading from the following folders:")
      foldersToRead.map(folder => s"$readRoot/$folder").foreach(println)
    }

    read(foldersToRead: _*)
  }

  override def readLatestPartitionUpToDateHour(upToDateHour: LocalDateTime, verbose: Boolean = false, isInclusive: Boolean = false): Dataset[IdentityAvailsRecord] = {
    val maxDateString = upToDateHour.toLocalDate.format(dateTimeFormat)
    val maxHourInt = upToDateHour.toLocalTime.format(hourTimeFormat).toInt

    // gotta look 2 folders deep for date/hour
    val allFolders = FSUtils.listDirectoryContents(s"$readRoot/$rootFolderPath")(spark)
      .filter(_.isDirectory)
      .flatMap(path => FSUtils.listDirectoryContents(path.getPath.toString)(spark))
      .map(_.getPath.toString.replace(s"$readRoot/$rootFolderPath", ""))

    val (bestDate, bestHour) = allFolders
      .flatMap { folder =>
        val matcher = dateHourFolderPathRegex.matcher(folder)
        if (matcher.find()) {
          val date = matcher.group(1)
          val hour = matcher.group(2)
          if ((date < maxDateString) || (date == maxDateString && (hour.toInt < maxHourInt || (isInclusive && hour.toInt == maxHourInt))))
            Some(date -> hour)
          else None
        }
        else None
      }
      .max

    val folderToRead = s"$rootFolderPath/$bestDate/$bestHour"
    if (verbose) {
      println(s"Reading from $folderToRead")
    }

    read(folderToRead)
  }

  override def read(rootFolderPaths: String*): Dataset[IdentityAvailsRecord] = {
    val paths = if (rootFolderPaths.nonEmpty) rootFolderPaths.map(path => s"$readRoot$path")
    else Seq(s"$readRoot$rootFolderPath")

    log.info(s"Reading from ${paths.mkString("'", ", ", "'")} as $readFormat")
    val dataFrame = spark.read
      .option("basePath", readRoot)
      .option("mergeSchema", mergeSchema)
      .parquet(paths: _*)

    dataFrame.as[IdentityAvailsRecord]
  }

  def readWithDateHourColumn(rootFolderPaths: String*): DataFrame = {
    val paths = if (rootFolderPaths.nonEmpty) rootFolderPaths.map(path => s"$readRoot$path")
    else Seq(s"$readRoot$rootFolderPath")

    log.info(s"Reading from ${paths.mkString("'", ", ", "'")} as $readFormat")
    val dataFrame = paths.map(p => {
      val matcher = dateHourFolderPathRegex.matcher(p.replace(s"$readRoot/$rootFolderPath", ""))
      matcher.find()
      val date = matcher.group(1)
      val hour = matcher.group(2)

      spark
        .read
        .schema(schema = schema)
        .option("basePath", "s3://ttd-identity/datapipeline")
        .option("mergeSchema", value = false)
        .parquet(p)
        .select('*, lit(date) as "date", lit(hour) as "hour")
    }
    ).reduce(_ unionByName  _)

    dataFrame
  }
}

case class IdentityAvailsRecord(
                                 uiid: String,
                                 uiidType: String,
                                 deviceType: String,
                                 ip: Array[Byte],
                                 ipFormat: String,
                                 ts: Long,
                                 country: String,
                                 os: String,
                                 connectionType: String,
                                 userAgent: String,
                                 dataOwner: String,
                                 eventType: String,
                                 eventGroup: String,
                                 eventOriginator: String,
                                 headers: Map[String, String],
                                 osVersion: String,
                                 browser: String,
                                 browserVersion: String,
                                 deviceMaker: String,
                                 deviceBrand: String,
                                 deviceModel: String,
                                 url: Seq[String],
                                 supplyVendorId: Integer,
                                 carrier: String,
                                 geoID: String,
                                 ST: String,
                                 DT: String,
                                 renderingContext: Byte,
                                 targetingIdType: Byte,
                                 dealIds: Seq[String],
                                 isRobot: Boolean,
                                 isDnt: Boolean,
                                 supplyVendorPublisherId: String,
                                 deviceIdType: Integer,
                                 region: String,
                                 optOutReason: Integer,
                                 longitude: Float,
                                 latitude: Float,
                                 isMaskedIp: Boolean,
                                 userType: String,
                                 unifiedId2: String,
                                 identityLinkId: String,
                               )