package com.thetradedesk.featurestore.utils

import com.thetradedesk.featurestore.configs.DataSource.crossDeviceDateFormatter
import com.thetradedesk.featurestore.rsm.CommonEnums.CrossDeviceVendor
import com.thetradedesk.featurestore.rsm.CommonEnums.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object CrossDeviceGraphUtil {
  def readGraphData[T <: Product : Manifest](date: LocalDate, crossDeviceVendor: CrossDeviceVendor)(implicit spark: SparkSession): DataFrame = {
    val (basePath, groupIdColumn) = crossDeviceVendor match {
      case CrossDeviceVendor.IAV2Person => ("s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph", "personId")
      case CrossDeviceVendor.IAV2Household => ("s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph_household", "householdID")
      case _ => throw new RuntimeException(s"${crossDeviceVendor} graph data is not supported yet")
    }

    for (i <- 0 to 13) {
      val sourcePath = s"$basePath/${date.minusDays(i).format(crossDeviceDateFormatter)}/_SUCCESS"

      if (FSUtils.fileExists(sourcePath)(spark)) {
        return spark
          .read
          .parquet(s"$basePath/${date.minusDays(i).format(crossDeviceDateFormatter)}/success")
          .select(col("uiid").as("TDID"), col(groupIdColumn).as("GroupId"))
      }
    }
    throw new RuntimeException(s"$crossDeviceVendor graph data is missing")
  }
}
