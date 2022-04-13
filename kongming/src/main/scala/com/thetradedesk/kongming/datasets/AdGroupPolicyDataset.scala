package com.thetradedesk.kongming.datasets

import org.apache.spark.sql.{Dataset, SparkSession}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

final case class AdGroupPolicyRecord(ConfigKey: String,
                                     ConfigValue: String,
                                     DataAggKey: String,
                                     DataAggValue: String,
                                     CrossDeviceUsage: Boolean,
                                     MinDailyConvCount: Int,
                                     MaxDailyConvCount: Int,
                                     LastTouchCount: Int,
                                     DataLookBack: Int,
                                     PositiveSamplingRate: Double,
                                     NegativeSamplingMethod: String,
                                     CrossDeviceConfidenceLevel: Double,
                                     FetchOlderTrainingData: Boolean,
                                     OldTrainingDataEpoch: Int
                                    )

object AdGroupPolicyDataset {
  val S3Path: String = "s3a://thetradedesk-useast-hadoop/cxw/foa/datasets/adgroupPolicies"
  def readHardCodedDataset(date: LocalDate)(implicit spark: SparkSession): Dataset[AdGroupPolicyRecord] = {
    spark.read
      .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(s"${S3Path}/date=${date.toString}")
      .selectAs[AdGroupPolicyRecord]
  }
}
