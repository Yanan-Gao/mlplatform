package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.{dryRun, getClassName, shouldTrackTDID, ttdEnv}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

final case class CrossDeviceGraphRecord(householdID: String,
                                        personID: String,
                                        screenID: String,
                                        screenIDType: Byte,
                                        uiid: String,
                                        uiidType: Byte,
                                        lastSeen: Long,
                                        score: Double,
                                        household_score: Double,
                                        country: String,
                                        deviceType: Byte,
                                        osFamily: Byte,
                                        date: String)

final case class LightCrossDeviceGraphRecord(personID: String,
                                             uiid: String,
                                             deviceType: Byte,
                                             score: Double)

final case class LightCrossDeviceHouseholdGraphRecord(householdID: String,
                                                      uiid: String,
                                                      deviceType: Byte,
                                                      score: Double)

case class LightCrossDeviceGraphDataset() extends
  LightReadableDataset[LightCrossDeviceGraphRecord]("sxd-etl/universal/iav2graph", "s3://thetradedesk-useast-data-import/", source = Some(DatasetSource.CrossDeviceGraph))

case class LightCrossDeviceHouseholdGraphDataset() extends
  LightReadableDataset[LightCrossDeviceHouseholdGraphRecord]("sxd-etl/universal/iav2graph_household", "s3://thetradedesk-useast-data-import/", source = Some(DatasetSource.CrossDeviceGraph))

final case class SampledCrossDeviceGraphRecord(
                                                personID: String,
                                                TDID: String,
                                                deviceType: Byte)

case class SampledCrossDeviceGraphDataset() extends
  LightReadableDataset[SampledCrossDeviceGraphRecord](s"/${config.getString(s"${getClassName(SampledCrossDeviceGraphDataset)}ReadEnv", ttdEnv)}/audience/sampledCrossDeviceGraph/v=1", S3Roots.ML_PLATFORM_ROOT)

object CrossDeviceGraphUtil {
  private def readGraphDataInternal[T <: Product : Manifest](date: LocalDate, dataset: LightReadableDataset[T])(implicit spark: SparkSession): DataFrame = {
    for (i <- 0 to 13) {
      val sourcePath = s"${dataset.basePath}/${date.minusDays(i).format(dataset.crossDeviceDateFormatter)}/_SUCCESS"

      if (FSUtils.fileExists(sourcePath)(spark)) {
        return dataset
          .readPartition(date.minusDays(i), lookBack = Some(0))
          .toDF()
      }
    }
    throw new RuntimeException(s"${dataset.getClass.getName} graph data is missing")
  }

  def readGraphData(date: LocalDate, crossDeviceVendor: CrossDeviceVendor, graphScoreThreshold: Double = 0.0, samplingFunction: Option[Column => Column] = None)(implicit spark: SparkSession): DataFrame = {
    val graphData = {
      if (crossDeviceVendor == CrossDeviceVendor.IAV2Person) {
        CrossDeviceGraphUtil
          .readGraphDataInternal(date, LightCrossDeviceGraphDataset())
          .where(shouldTrackTDID('uiid) && 'score > lit(graphScoreThreshold))
          .select('uiid.alias("TDID"), 'personId.alias("groupId"))
      } else if (crossDeviceVendor == CrossDeviceVendor.IAV2Household) {
        CrossDeviceGraphUtil
          .readGraphDataInternal(date, LightCrossDeviceHouseholdGraphDataset())
          .where(shouldTrackTDID('uiid) && 'score > lit(graphScoreThreshold))
          .select('uiid.alias("TDID"), 'householdID.alias("groupId"))
      } else {
        throw new UnsupportedOperationException(s"crossDeviceVendor ${crossDeviceVendor} is not supported")
      }
    }

    if (dryRun && samplingFunction.nonEmpty) {
      val samplingUDF = samplingFunction.get
      graphData.where(samplingUDF('groupId))
    } else {
      graphData
    }
  }
}