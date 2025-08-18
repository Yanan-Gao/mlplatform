package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.loadParquetDataWithHourPart
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import job.GenerateTrainSetLastTouch.{conversionLookback, incTrain}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.broadcast
import com.thetradedesk.spark.datasets.core.DefaultTimeFormatStrings
import scala.util.{Try, Success, Failure}

import java.time.LocalDate


object PrintLengthKongming extends KongmingBaseJob {

  override def jobName: String = "PrintLengthKongming"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    /**
     * This job is designed to test the Metastore integration.
     * Since we plan to continue developing and debugging Metastore-related features,
     * it's important to verify that reading from Metastore yields the same results as reading from S3.
     *
     * To test this:
     * - Run the job on EMR with the JVM flag `-DenableMetastore=true` to enable Metastore reads.
     * - Then run it again with `-DenableMetastore=false` to fall back to S3 reads.
     * - Compare the outputs from various read functions to ensure consistency between the two sources.
     */

    testReadRange(date, 3)
    testReadDate(date)
    testReadBackFromDate(date, 3, 4)
    testReadPartition(date)
    testReadLatestPartitionUpTo(date)
    testReadLatestPartition()

    Array(("", 0))
  }

  def testReadRange(
                     date: LocalDate,
                     lookbackDays: Int
                   ): Array[(String, Long)] = {

    println("++++++++++++ TEST `readRange` ++++++++++++")

    val startDate = date.minusDays(lookbackDays)

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset(), true),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset(), true),
      ("CvrForScalingDataset", () => CvrForScalingDataset(), true),
      ("DailyAttributionDataset", () => DailyAttributionDataset(), true),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset(), true),
      ("DailyClickDataset", () => DailyClickDataset(), true),
      ("DailyConversionDataset", () => DailyConversionDataset(), false),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset(), false),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset(), false)
    )

    datasets.map { case (name, constructor, isInclusive) =>
      val df = constructor().readRange(startDate, date, isInclusive = isInclusive)
      val count = df.count()
      println(s"$name count: $count")
      println("------------------------------------------------------------")
      name -> count
    }.toArray
  }

  def testReadDate(date: LocalDate): Array[(String, Long)] = {

    println("++++++++++++ TEST `readDate` ++++++++++++")

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset()),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset()),
      ("CvrForScalingDataset", () => CvrForScalingDataset()),
      ("DailyAttributionDataset", () => DailyAttributionDataset()),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset()),
      ("DailyClickDataset", () => DailyClickDataset()),
      ("DailyConversionDataset", () => DailyConversionDataset()),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset()),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset())
    )

    datasets.map { case (name, constructor) =>
      val df = constructor().readDate(date)
      val count = df.count()
      println(s"$name count: $count")
      println("------------------------------------------------------------")
      name -> count
    }.toArray

    val datasets2 = Seq(
      ("DailyAttributionEventsDataset", () => DailyAttributionEventsDataset()),
      ("DailyHourlyBidsImpressionsDataset", () => DailyHourlyBidsImpressionsDataset()),
      ("DataCsvForModelTrainingDatasetLastTouch", () => DataCsvForModelTrainingDatasetLastTouch()),
      ("DataCsvForModelTrainingDatasetClick", () => DataCsvForModelTrainingDatasetClick()),
      ("DataIncCsvForModelTrainingDatasetLastTouch", () => DataIncCsvForModelTrainingDatasetLastTouch()),
      ("DataIncCsvForModelTrainingDatasetClick", () => DataIncCsvForModelTrainingDatasetClick()),
      ("UserDataIncCsvForModelTrainingDatasetLastTouch", () => UserDataIncCsvForModelTrainingDatasetLastTouch()),
      ("UserDataCsvForModelTrainingDatasetLastTouch", () => UserDataCsvForModelTrainingDatasetLastTouch()),
      ("UserDataCsvForModelTrainingDatasetClick", () => UserDataCsvForModelTrainingDatasetClick()),
      ("UserDataIncCsvForModelTrainingDatasetClick", () => UserDataIncCsvForModelTrainingDatasetClick()),
      ("OutOfSampleAttributionDataset", () => OutOfSampleAttributionDataset(delayNDays = 10)),
      ("OldOutOfSampleAttributionDataset", () => OldOutOfSampleAttributionDataset(delayNDays = 10))
    )

    datasets2.map { case (name, constructor) =>
      val result = Try {
        val datePartition = if (name == "OutOfSampleAttributionDataset" || name == "OldOutOfSampleAttributionDataset") {
          date.minusDays(14)
        } else {
          date
        }
        val df = constructor().readDate(datePartition)
        val count = df.count()
        println(s"$name count: $count")

        println(s"$name preview (first 2 rows):")
        df.take(2).foreach(row => println(row))
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray
  }

  def testReadBackFromDate(date: LocalDate, numberOfDaysToRead: Int, maxExtraCheckDays: Int): Array[(String, Long)] = {

    println("++++++++++++ TEST `readBackFromDate` ++++++++++++")

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset(), true),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset(), true),
      ("CvrForScalingDataset", () => CvrForScalingDataset(), true),
      ("DailyAttributionDataset", () => DailyAttributionDataset(), true),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset(), true),
      ("DailyClickDataset", () => DailyClickDataset(), true),
      ("DailyConversionDataset", () => DailyConversionDataset(), false),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset(), false),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset(), false)
    )

    datasets.map { case (name, constructor, isInclusive) =>
      val result = Try {
        val df = constructor().readBackFromDate(date, numberOfDaysToRead, maxExtraCheckDays, isInclusive = isInclusive)
        val count = df.count()
        println(s"$name count: $count")
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray
  }

  def testReadPartition(date: LocalDate): Array[(String, Long)] = {

    println("++++++++++++ TEST `readPartition` ++++++++++++")

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset()),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset()),
      ("CvrForScalingDataset", () => CvrForScalingDataset()),
      ("DailyAttributionDataset", () => DailyAttributionDataset()),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset()),
      ("DailyClickDataset", () => DailyClickDataset()),
      ("DailyConversionDataset", () => DailyConversionDataset()),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset()),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset())
    )

    datasets.map { case (name, constructor) =>
      val result = Try {
        val df = constructor().readPartition(date)
        val count = df.count()
        println(s"$name count: $count")

        println(s"$name preview (first 2 rows):")
        df.take(2).foreach(row => println(row))
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray

    val ImpDate = date.minusDays(3).format(DefaultTimeFormatStrings.dateTimeFormatter)
    val datasets2 = Seq(
      ("DailyAttributionEventsDataset", () => DailyAttributionEventsDataset(), ImpDate),
      ("DailyHourlyBidsImpressionsDataset", () => DailyHourlyBidsImpressionsDataset(), "00"),
      ("DataCsvForModelTrainingDatasetLastTouch", () => DataCsvForModelTrainingDatasetLastTouch(), "train"),
      ("DataCsvForModelTrainingDatasetClick", () => DataCsvForModelTrainingDatasetClick(), "train"),
      ("DataIncCsvForModelTrainingDatasetLastTouch", () => DataIncCsvForModelTrainingDatasetLastTouch(), "train"),
      ("DataIncCsvForModelTrainingDatasetClick", () => DataIncCsvForModelTrainingDatasetClick(), "train"),
      ("UserDataIncCsvForModelTrainingDatasetLastTouch", () => UserDataIncCsvForModelTrainingDatasetLastTouch(), "train"),
      ("UserDataCsvForModelTrainingDatasetLastTouch", () => UserDataCsvForModelTrainingDatasetLastTouch(), "train"),
      ("UserDataCsvForModelTrainingDatasetClick", () => UserDataCsvForModelTrainingDatasetClick(), "train"),
      ("UserDataIncCsvForModelTrainingDatasetClick", () => UserDataIncCsvForModelTrainingDatasetClick(), "train"),
      ("OutOfSampleAttributionDataset", () => OutOfSampleAttributionDataset(delayNDays = 10), "tracked"),
      ("OldOutOfSampleAttributionDataset", () => OldOutOfSampleAttributionDataset(delayNDays = 10), "tracked")
    )

    datasets2.map { case (name, constructor, split) =>
      val result = Try {

        val datePartition = if (name == "OutOfSampleAttributionDataset" || name == "OldOutOfSampleAttributionDataset") {
          date.minusDays(14)
        } else {
          date
        }

        val df = constructor().readPartition(datePartition, split)
        val count = df.count()
        println(s"$name count: $count")

        println(s"$name preview (first 2 rows):")
        df.take(2).foreach(row => println(row))
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray
  }

  def testReadLatestPartitionUpTo(date: LocalDate): Array[(String, Long)] = {

    println("++++++++++++ TEST `readLatestPartitionUpTo` ++++++++++++")

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset()),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset()),
      ("CvrForScalingDataset", () => CvrForScalingDataset()),
      ("DailyAttributionDataset", () => DailyAttributionDataset()),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset()),
      ("DailyClickDataset", () => DailyClickDataset()),
      ("DailyConversionDataset", () => DailyConversionDataset()),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset()),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset())
    )

    datasets.map { case (name, constructor) =>
      val result = Try {
        val df = constructor().readLatestPartitionUpTo(date)
        val count = df.count()
        println(s"$name count: $count")
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray
  }

  def testReadLatestPartition(): Array[(String, Long)] = {

    println("++++++++++++ TEST `readLatestPartition` ++++++++++++")

    val datasets = Seq(
      ("AdGroupPolicyDataset", () => AdGroupPolicyDataset()),
      ("AdGroupPolicyMappingDataset", () => AdGroupPolicyMappingDataset()),
      ("CvrForScalingDataset", () => CvrForScalingDataset()),
      ("DailyAttributionDataset", () => DailyAttributionDataset()),
      ("DailyBidFeedbackDataset", () => DailyBidFeedbackDataset()),
      ("DailyClickDataset", () => DailyClickDataset()),
      ("DailyConversionDataset", () => DailyConversionDataset()),
      ("DailyOfflineScoringDataset", () => DailyOfflineScoringDataset()),
      ("SampledImpressionForIsotonicRegDataset", () => SampledImpressionForIsotonicRegDataset()),
    )

    datasets.map { case (name, constructor) =>
      val result = Try {
        val df = constructor().readLatestPartition()
        val count = df.count()
        println(s"$name count: $count")
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray

    val datasets2 = Seq(
      ("DailyAttributionEventsDataset", () => DailyAttributionEventsDataset()),
      ("DailyHourlyBidsImpressionsDataset", () => DailyHourlyBidsImpressionsDataset()),
      ("DataCsvForModelTrainingDatasetLastTouch", () => DataCsvForModelTrainingDatasetLastTouch()),
      ("DataCsvForModelTrainingDatasetClick", () => DataCsvForModelTrainingDatasetClick()),
      ("DataIncCsvForModelTrainingDatasetLastTouch", () => DataIncCsvForModelTrainingDatasetLastTouch()),
      ("DataIncCsvForModelTrainingDatasetClick", () => DataIncCsvForModelTrainingDatasetClick()),
      ("UserDataIncCsvForModelTrainingDatasetLastTouch", () => UserDataIncCsvForModelTrainingDatasetLastTouch()),
      ("UserDataCsvForModelTrainingDatasetLastTouch", () => UserDataCsvForModelTrainingDatasetLastTouch()),
      ("UserDataCsvForModelTrainingDatasetClick", () => UserDataCsvForModelTrainingDatasetClick()),
      ("UserDataIncCsvForModelTrainingDatasetClick", () => UserDataIncCsvForModelTrainingDatasetClick()),
      ("OutOfSampleAttributionDataset", () => OutOfSampleAttributionDataset(delayNDays = 10)),
      ("OldOutOfSampleAttributionDataset", () => OldOutOfSampleAttributionDataset(delayNDays = 10))
    )

    datasets2 .map { case (name, constructor) =>
      val result = Try {
        val df = constructor().readLatestPartition()
        val count = df.count()
        println(s"$name count: $count")
        count
      }

      result match {
        case Success(count) => println("------------------------------------------------------------"); name -> count
        case Failure(e) =>
          println(s"[ERROR] $name failed: ${e.getMessage}")
          println("------------------------------------------------------------")
          name -> -1L
      }
    }.toArray
  }
}