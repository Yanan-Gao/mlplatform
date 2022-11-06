package com.thetradedesk.audience.transform

import com.thetradedesk.audience.datasets.{TargetingDataRecord, TrackingTagRecord, UniversalPixelRecord, UniversalPixelTrackingTagRecord}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/* FirstPartyDataTypeId enum
    Keyword: 1
    ImagePixel: 2
    FixedPriceUser: 3
    EmailOpen: 4
    UniversalPixel: 5
    IPAddressRange: 6
    AppEngagement: 7
    ImportedAdvertiserData: 8
    ImportedAdvertiserDataWithBaseBid: 9
    HouseholdExtension: 11
    ClickRetargeting: 12
    VideoEvent_Start: 13
    VideoEvent_Midpoint: 14
    VideoEvent_Complete: 15
    DirectIPTargeting: 16
    DynamicTrackingRule: 17
    BulkUserList: 18
    EcommerceCatalogList: 19
    CrmData: 20
    CampaignSeedData: 21
 */
object FirstPartyDataTransform {
  def tagSegment(
    trackingTag: Dataset[TrackingTagRecord],
    universalPixel: Dataset[UniversalPixelRecord],
    universalPixelTrackingTag: Dataset[UniversalPixelTrackingTagRecord],
    targetingData: Dataset[TargetingDataRecord]
  ): Dataset[_] = {
    val pixels = trackingTag.join(universalPixelTrackingTag.join(universalPixel, Seq("UPixelId")), Seq("TrackingTagId"), "left")
      .withColumn("FirstPartyDataTypeId",
        when('UPixelId.isNull, 2)
          .when('TrackedAppVendorId.isNull && !'UPixelId.isNull, 5)
          .when(!'TrackedAppVendorId.isNull && !'UPixelId.isNull, 7))
    targetingData.join(pixels, Seq("TargetingDataId"), "left")
      .withColumn("FirstPartyDataTypeId", when('FirstPartyDataTypeId.isNull, 'DataTypeId).otherwise('FirstPartyDataTypeId))
      .select('TargetingDataId, 'FirstPartyDataTypeId)
  }
}
