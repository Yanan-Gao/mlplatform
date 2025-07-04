package com.thetradedesk.audience.utils

import com.thetradedesk.audience.datasets.{CrossDeviceGraphUtil, CrossDeviceVendor}
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.{Sampler, TrackableUserSampler}
import com.thetradedesk.audience.{dateFormatter, ttdReadEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, typedLit}

import java.time.LocalDate

object FeatureStoreUtils {
  /**
   * read and join graph extended density feature
   * df must include TDID column
   *
   * @return
   */
  def joinDensityFeature(
                          date: LocalDate,
                          df: DataFrame,
                          joinType: String = "left",
                          sampler: Sampler = TrackableUserSampler,
                          crossDeviceVendor: CrossDeviceVendor = CrossDeviceVendor.None,
                          env: String = ttdReadEnv,
                          split: Option[Int] = None,
                        ): DataFrame = {
    if (!df.columns.contains("TDID")) {
      throw new RuntimeException("input data frame must contain TDID column for join")
    }
    val dateStr = date.format(dateFormatter)
    val resultDF = crossDeviceVendor match {
      case CrossDeviceVendor.None =>
        val densityFeatureDF = if (split.isEmpty) {
          spark.read.parquet(s"s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/$env/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=$dateStr")
        } else {
          spark.read.parquet(s"s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/$env/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1/date=$dateStr/split=${split.get}/")
        }
        df
          .join(
            densityFeatureDF
              .select("TDID", "SyntheticId_Level1", "SyntheticId_Level2")
              .where(sampler.samplingFunction('TDID)), Seq("TDID"), joinType)
      case _ =>
        val graphDensityFeatureDf = spark.read.parquet(s"s3://thetradedesk-mlplatform-us-east-1/features/feature_store/$env/profiles/source=bidsimpression/index=GroupId/job=DailyGroupIDDensityScoreSplitJob/XDV=${crossDeviceVendor.toString}/v=1/date=$dateStr/")
          .select("GroupId", "SyntheticId_Level1", "SyntheticId_Level2")
        val graph = CrossDeviceGraphUtil.readGraphData(date, crossDeviceVendor)
        df
          .join(
            graph.where(sampler.samplingFunction('TDID)), Seq("TDID"), joinType
          )
          .withColumn("GroupId", coalesce(col("GroupId"), col("TDID")))
          .join(
            graphDensityFeatureDf, Seq("GroupId"), joinType
          )
    }

    resultDF
      .withColumn("SyntheticId_Level1", coalesce(col("SyntheticId_Level1"), typedLit(Array.empty[Int])))
      .withColumn("SyntheticId_Level2", coalesce(col("SyntheticId_Level2"), typedLit(Array.empty[Int])))
  }
}
