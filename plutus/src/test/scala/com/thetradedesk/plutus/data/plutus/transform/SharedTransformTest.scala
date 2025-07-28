package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adFormatMock, pcResultsMergedMock, platformReportMock}
import com.thetradedesk.plutus.data.transform.SharedTransforms.{AddChannel, AddChannelUsingAdFormat, AddDeviceTypeIdAndRenderingContextId}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdFormatRecord
import org.apache.spark.sql.functions._


class SharedTransformTest extends TTDSparkTest {
  test("AddChannelUsingAdFormat outputs expected Channel value using RenderingContextId & DeviceTypeId") {
    val pcResultsDataset = Seq(pcResultsMergedMock()).toDF()
      .withColumn("AdFormat", concat(col("AdWidthInPixels"), lit("x"), col("AdHeightInPixels")))
      .drop("MediaTypeId")
    val adFormatData = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord]

    val addChannelToPcResultsDataset =
      AddChannelUsingAdFormat(
        pcResultsDataset,
        adFormatData,
        renderingContextCol = "RenderingContext",
        deviceTypeCol = "DeviceType"
      )

    val res_addChannelToPcResultsDataset = addChannelToPcResultsDataset.collectAsList()
    val res = res_addChannelToPcResultsDataset.get(0)

    assert(res.getAs[String]("ChannelSimple") == "Display", "Validating correct use of AddChannelUsingAdFormat")
  }

  test("AddChannel outputs expected Channel value using RenderingContextId & DeviceTypeId") {
    val pcResultsDataset = Seq(pcResultsMergedMock(deviceType = 7)).toDF()

    val addChannelToPcResultsDataset =
      AddChannel(
        pcResultsDataset,
        renderingContextCol = "RenderingContext",
        deviceTypeCol = "DeviceType"
      )

    val res_addChannelToPcResultsDataset = addChannelToPcResultsDataset.collectAsList()
    val res = res_addChannelToPcResultsDataset.get(0)

    assert(res.getAs[String]("ChannelSimple") == "Digital Out Of Home", "Validating correct use of AddChannel")
  }

  test("AddChannel outputs expected Channel value using RenderingContextId & DeviceTypeId: case requiring AddDeviceTypeIdAndRenderingContextId") {
    val platformReportData = platformReportMock(renderingContext = Some("Other"), deviceType = Some("OutOfHome")).toDF()

    val addChannelToPlatformReportData =
      AddChannel(
        AddDeviceTypeIdAndRenderingContextId(
          platformReportData,
          renderingContextCol = "RenderingContext",
          deviceTypeCol = "DeviceType"
        ),
        renderingContextCol = "RenderingContextId",
        deviceTypeCol = "DeviceTypeId"
      )

    val res_addChannelToPlatformReportData = addChannelToPlatformReportData.collectAsList()
    val res = res_addChannelToPlatformReportData.get(0)

    assert(res.getAs[String]("ChannelSimple") == "Digital Out Of Home", "Validating correct use of AddChannel requiring AddDeviceTypeIdAndRenderingContextId")
  }

  test("AddChannel outputs wrong value based on RenderingContext and DeviceType Strings: case requiring AddDeviceTypeIdAndRenderingContextId but not being used") {
    val platformReportData = platformReportMock(renderingContext = Some("MobileOptimizedWeb"), deviceType = Some("OutOfHome")).toDF()

    val addChannelToPlatformReportData = AddChannel(platformReportData, renderingContextCol = "RenderingContext", deviceTypeCol = "DeviceType")

    val res_addChannelToPlatformReportData = addChannelToPlatformReportData.collectAsList()
    val res = res_addChannelToPlatformReportData.get(0)

    assert(res.getAs[String]("ChannelSimple") == "Display", "Validating incorrect use of AddChannel - should be DOOH instead of Display")
  }
}
