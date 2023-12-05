package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.CrossDeviceVendor.{Adbrain, CrossDeviceVendor}
import com.thetradedesk.audience.ttdEnv
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  LightReadableDataset[SampledCrossDeviceGraphRecord](s"/${ttdEnv}/audience/sampledCrossDeviceGraph/v=1", S3Roots.ML_PLATFORM_ROOT)

object CrossDeviceGraphUtil {
  def readGraphData[T <: Product : Manifest](date: LocalDate, dataset: LightReadableDataset[T])(implicit spark: SparkSession): DataFrame = {
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
}