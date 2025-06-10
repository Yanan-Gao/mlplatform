package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate


object HourlyAggImpressions extends FeatureStoreAggJob {

  override val sourcePartition: String = "bidsimpression"

  def aggregateFeatures(df: DataFrame, features: Seq[String]): DataFrame = {
    // Stack data for all features into one DataFrame
    val stackedDf = features.map { feature =>
      df.select(
        col("TDID"),
        lit(feature).alias("FeatureKey"),
        col(s"${feature}Hashed").alias("FeatureValueHashed")
      )
    }.reduce(_ union _)

//     TODO: Allow for configurable aggregations instead of just "count"
//     Code below will help with this, idea is to collect a list of aggregation expressions
//     and then pass that to .agg() call
//
//    val aggExpr = aggFunction match {
//      case AggFunc.Count => count("*").alias("FeatureCount")
//      case AggFunc.Sum => sum(col("FeatureValueHashed")).alias("FeatureSum")
//      case _ => throw new IllegalArgumentException(s"Unsupported aggregation function: $aggFunction")
//    }

    stackedDf.groupBy(
      col("TDID"),
      col("FeatureKey"),
      col("FeatureValueHashed")
    ).agg(count("*").alias("FeatureCount"))
  }

  def readBidsImpressions(features: List[String], date: LocalDate, hour: Option[Int]) = {
    val yyyy = date.getYear.toString
    val mm = f"${date.getMonthValue}%02d"
    val dd = f"${date.getDayOfMonth}%02d"

    try {
      val bidsImpsPath = s"${BidsImpressions.BIDSIMPRESSIONSS3}/prod/bidsimpressions/year=$yyyy/month=$mm/day=$dd/"

      var bidsImpressions = hour match {
        case Some(h) => spark.read.parquet(bidsImpsPath + s"hourPart=$h/")
        case _ => spark.read.parquet(bidsImpsPath)
      }

      bidsImpressions = bidsImpressions
        .select("UIID", features: _*)
        .withColumnRenamed("UIID", "TDID")
        .filter(col("TDID") =!= "00000000-0000-0000-0000-000000000000")
        .filter($"IsImp")

      features.foreach { feature =>
        bidsImpressions = bidsImpressions.withColumn(
          s"${feature}Hashed",
          when(
            col(feature).isNull,
            lit(null)
          ).otherwise(
            xxhash64(concat(col(feature)), lit(salt))
          )
        )
      }

      bidsImpressions.drop(features: _*)
    }
    catch {
      case e: Throwable =>
        println("Bad column names. Please provide a valid job config!")
        throw (e)
    }
  }

  // TODO: refactor this away
  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    spark.read.parquet("")
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    for (i <- hourArray) {
      runTransformHour(args, hourInt = i)
    }

    // TODO: return valid row count here with job name
    Array(("", 0))
  }

  def runTransformHour(args: Array[String], hourInt: Int): Unit = {
    val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=${jobName}/v=1/date=${getDateStr(date)}/hour=$hourInt/"
    val successFile = s"$writePath/_SUCCESS"

    // skip processing this split if data from a previous run already exists
    if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
      println(s"split ${splitIndex} data is existing")
      return
    }

    val features = catFeatSpecs.map { f => f.aggField }.toList

    val bidImps = readBidsImpressions(features, date, Some(hourInt))

    val aggregatedDf = aggregateFeatures(bidImps, features)

    aggregatedDf.repartition(defaultNumPartitions).write.mode(SaveMode.Overwrite).parquet(writePath)
  }
}
