package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core._

final case class UserDataForModelTrainingRecord(
                                                 AdGroupIdStr: String,
                                                 AdGroupId: Int,
                                                 CampaignIdStr: String,
                                                 CampaignId: Int,
                                                 AdvertiserIdStr: String,
                                                 AdvertiserId: Int,
                                                 BidRequestIdStr: String,
                                                 Weight: Float,
                                                 Target: Float,
                                                 Revenue: Option[Float],

                                                 SupplyVendor: Option[Int],
                                                 SupplyVendorPublisherId: Option[Int],
                                                 AliasedSupplyPublisherId: Option[Int],
                                                 //                                 ImpressionPlacementId: Option[String],
                                                 Site: Option[Int],
                                                 AdFormat: Int,

                                                 Country: Option[Int],
                                                 Region: Option[Int],
                                                 City: Option[Int],
                                                 Zip: Option[Int],

                                                 DeviceMake: Option[Int],
                                                 DeviceModel: Option[Int],
                                                 RequestLanguages: Int,

                                                 RenderingContext: Option[Int],
                                                 DeviceType: Option[Int],
                                                 OperatingSystem: Option[Int],
                                                 Browser: Option[Int],
                                                 InternetConnectionType: Option[Int],
                                                 MatchedFoldPosition: Int,

                                                 HasContextualCategoryTier1: Int,
                                                 ContextualCategoryLengthTier1: Float,

                                                 ContextualCategoriesTier1_Column0: Int,
                                                 ContextualCategoriesTier1_Column1: Int,
                                                 ContextualCategoriesTier1_Column2: Int,
                                                 ContextualCategoriesTier1_Column3: Int,
                                                 ContextualCategoriesTier1_Column4: Int,
                                                 ContextualCategoriesTier1_Column5: Int,
                                                 ContextualCategoriesTier1_Column6: Int,
                                                 ContextualCategoriesTier1_Column7: Int,
                                                 ContextualCategoriesTier1_Column8: Int,
                                                 ContextualCategoriesTier1_Column9: Int,
                                                 ContextualCategoriesTier1_Column10: Int,
                                                 ContextualCategoriesTier1_Column11: Int,
                                                 ContextualCategoriesTier1_Column12: Int,
                                                 ContextualCategoriesTier1_Column13: Int,
                                                 ContextualCategoriesTier1_Column14: Int,
                                                 ContextualCategoriesTier1_Column15: Int,
                                                 ContextualCategoriesTier1_Column16: Int,
                                                 ContextualCategoriesTier1_Column17: Int,
                                                 ContextualCategoriesTier1_Column18: Int,
                                                 ContextualCategoriesTier1_Column19: Int,
                                                 ContextualCategoriesTier1_Column20: Int,
                                                 ContextualCategoriesTier1_Column21: Int,
                                                 ContextualCategoriesTier1_Column22: Int,
                                                 ContextualCategoriesTier1_Column23: Int,
                                                 ContextualCategoriesTier1_Column24: Int,
                                                 ContextualCategoriesTier1_Column25: Int,
                                                 ContextualCategoriesTier1_Column26: Int,
                                                 ContextualCategoriesTier1_Column27: Int,
                                                 ContextualCategoriesTier1_Column28: Int,
                                                 ContextualCategoriesTier1_Column29: Int,
                                                 ContextualCategoriesTier1_Column30: Int,

                                                 sin_hour_week: Float,
                                                 cos_hour_week: Float,
                                                 sin_hour_day: Float,
                                                 cos_hour_day: Float,
                                                 sin_minute_hour: Float,
                                                 cos_minute_hour: Float,
                                                 latitude: Option[Float],
                                                 longitude: Option[Float],

                                                 IndustryCategoryId: Option[Int],
                                                 AudienceId_Column0: Int,
                                                 AudienceId_Column1: Int,
                                                 AudienceId_Column2: Int,
                                                 AudienceId_Column3: Int,
                                                 AudienceId_Column4: Int,
                                                 AudienceId_Column5: Int,
                                                 AudienceId_Column6: Int,
                                                 AudienceId_Column7: Int,
                                                 AudienceId_Column8: Int,
                                                 AudienceId_Column9: Int,
                                                 AudienceId_Column10: Int,
                                                 AudienceId_Column11: Int,
                                                 AudienceId_Column12: Int,
                                                 AudienceId_Column13: Int,
                                                 AudienceId_Column14: Int,
                                                 AudienceId_Column15: Int,
                                                 AudienceId_Column16: Int,
                                                 AudienceId_Column17: Int,
                                                 AudienceId_Column18: Int,
                                                 AudienceId_Column19: Int,
                                                 AudienceId_Column20: Int,
                                                 AudienceId_Column21: Int,
                                                 AudienceId_Column22: Int,
                                                 AudienceId_Column23: Int,
                                                 AudienceId_Column24: Int,
                                                 AudienceId_Column25: Int,
                                                 AudienceId_Column26: Int,
                                                 AudienceId_Column27: Int,
                                                 AudienceId_Column28: Int,
                                                 AudienceId_Column29: Int,

                                                 UserData_Column0: Int,
                                                 UserData_Column1: Int,
                                                 UserData_Column2: Int,
                                                 UserData_Column3: Int,
                                                 UserData_Column4: Int,
                                                 UserData_Column5: Int,
                                                 UserData_Column6: Int,
                                                 UserData_Column7: Int,
                                                 UserData_Column8: Int,
                                                 UserData_Column9: Int,
                                                 UserData_Column10: Int,
                                                 UserData_Column11: Int,
                                                 UserData_Column12: Int,
                                                 UserData_Column13: Int,
                                                 UserData_Column14: Int,
                                                 UserData_Column15: Int,
                                                 UserData_Column16: Int,
                                                 UserData_Column17: Int,
                                                 UserData_Column18: Int,
                                                 UserData_Column19: Int,
                                                 UserData_Column20: Int,
                                                 UserData_Column21: Int,
                                                 UserData_Column22: Int,
                                                 UserData_Column23: Int,
                                                 UserData_Column24: Int,
                                                 UserData_Column25: Int,
                                                 UserData_Column26: Int,
                                                 UserData_Column27: Int,
                                                 UserData_Column28: Int,
                                                 UserData_Column29: Int,
                                                 UserData_Column30: Int,
                                                 UserData_Column31: Int,
                                                 UserData_Column32: Int,
                                                 UserData_Column33: Int,
                                                 UserData_Column34: Int,
                                                 UserData_Column35: Int,
                                                 UserData_Column36: Int,
                                                 UserData_Column37: Int,
                                                 UserData_Column38: Int,
                                                 UserData_Column39: Int,
                                                 UserData_Column40: Int,
                                                 UserData_Column41: Int,
                                                 UserData_Column42: Int,
                                                 UserData_Column43: Int,
                                                 UserData_Column44: Int,
                                                 UserData_Column45: Int,
                                                 UserData_Column46: Int,
                                                 UserData_Column47: Int,
                                                 UserData_Column48: Int,
                                                 UserData_Column49: Int,
                                                 UserData_Column50: Int,
                                                 UserData_Column51: Int,
                                                 UserData_Column52: Int,
                                                 UserData_Column53: Int,
                                                 UserData_Column54: Int,
                                                 UserData_Column55: Int,
                                                 UserData_Column56: Int,
                                                 UserData_Column57: Int,
                                                 UserData_Column58: Int,
                                                 UserData_Column59: Int,
                                                 UserData_Column60: Int,
                                                 UserData_Column61: Int,
                                                 UserData_Column62: Int,
                                                 UserData_Column63: Int,
                                                 UserData_Column64: Int,
                                                 UserData_Column65: Int,
                                                 UserData_Column66: Int,
                                                 UserData_Column67: Int,
                                                 UserData_Column68: Int,
                                                 UserData_Column69: Int,
                                                 UserData_Column70: Int,
                                                 UserData_Column71: Int,
                                                 UserData_Column72: Int,
                                                 UserData_Column73: Int,
                                                 UserData_Column74: Int,
                                                 UserData_Column75: Int,
                                                 UserData_Column76: Int,
                                                 UserData_Column77: Int,
                                                 UserData_Column78: Int,
                                                 UserData_Column79: Int,
                                                 UserData_Column80: Int,
                                                 UserData_Column81: Int,
                                                 UserData_Column82: Int,
                                                 UserData_Column83: Int,
                                                 UserData_Column84: Int,
                                                 UserData_Column85: Int,
                                                 UserData_Column86: Int,
                                                 UserData_Column87: Int,
                                                 UserData_Column88: Int,
                                                 UserData_Column89: Int,
                                                 UserData_Column90: Int,
                                                 UserData_Column91: Int,
                                                 UserData_Column92: Int,
                                                 UserData_Column93: Int,
                                                 UserData_Column94: Int,
                                                 UserData_Column95: Int,
                                                 UserData_Column96: Int,
                                                 UserData_Column97: Int,
                                                 UserData_Column98: Int,
                                                 UserData_Column99: Int,
                                                 UserData_Column100: Int,
                                                 UserData_Column101: Int,
                                                 UserData_Column102: Int,
                                                 UserData_Column103: Int,
                                                 UserData_Column104: Int,
                                                 UserData_Column105: Int,
                                                 UserData_Column106: Int,
                                                 UserData_Column107: Int,
                                                 UserData_Column108: Int,
                                                 UserData_Column109: Int,
                                                 UserData_Column110: Int,
                                                 UserData_Column111: Int,
                                                 UserData_Column112: Int,
                                                 UserData_Column113: Int,
                                                 UserData_Column114: Int,
                                                 UserData_Column115: Int,
                                                 UserData_Column116: Int,
                                                 UserData_Column117: Int,
                                                 UserData_Column118: Int,
                                                 UserData_Column119: Int,
                                                 UserData_Column120: Int,
                                                 UserData_Column121: Int,
                                                 UserData_Column122: Int,
                                                 UserData_Column123: Int,
                                                 UserData_Column124: Int,
                                                 UserData_Column125: Int,
                                                 UserData_Column126: Int,
                                                 UserData_Column127: Int,
                                                 UserData_Column128: Int,
                                                 UserData_Column129: Int,
                                                 UserData_Column130: Int,
                                                 UserData_Column131: Int,
                                                 UserData_Column132: Int,
                                                 UserData_Column133: Int,
                                                 UserData_Column134: Int,
                                                 UserData_Column135: Int,
                                                 UserData_Column136: Int,
                                                 UserData_Column137: Int,
                                                 UserData_Column138: Int,
                                                 UserData_Column139: Int,
                                                 UserData_Column140: Int,
                                                 //UserData_Column141: Int,
                                                 //UserData_Column142: Int,
                                                 //UserData_Column143: Int,
                                                 //UserData_Column144: Int,
                                                 //UserData_Column145: Int,
                                                 //UserData_Column146: Int,
                                                 //UserData_Column147: Int,
                                                 //UserData_Column148: Int,
                                                 //UserData_Column149: Int,
                                                 //UserData_Column150: Int,
//                                                 UserData_Column151: Int,

                                                 HasUserData: Int,
                                                 UserDataLength: Float,
                                                 UserDataOptIn: Int,
                                                 IdType: Int,
                                                 IdCount: Int,
                                                 UserAgeInDays: Option[Float]
                                               // 256 columns in total
                                               )

final case class ArrayUserDataForModelTrainingRecord(
                                                 AdGroupIdEncoded: Long,
                                                 AdGroupId: Int,
                                                 CampaignIdEncoded: Long,
                                                 CampaignId: Int,
                                                 AdvertiserIdEncoded: Long,
                                                 AdvertiserId: Int,
                                                 Weight: Float,
                                                 Target: Float,
                                                 Revenue: Option[Float],

                                                 SupplyVendor: Option[Int],
                                                 SupplyVendorPublisherId: Option[Int],
                                                 AliasedSupplyPublisherId: Option[Int],
                                                 // ImpressionPlacementId: Option[String],
                                                 Site: Option[Int],
                                                 AdFormat: Int,

                                                 Country: Option[Int],
                                                 Region: Option[Int],
                                                 City: Option[Int],
                                                 Zip: Option[Int],

                                                 DeviceMake: Option[Int],
                                                 DeviceModel: Option[Int],
                                                 RequestLanguages: Int,

                                                 RenderingContext: Option[Int],
                                                 DeviceType: Option[Int],
                                                 OperatingSystem: Option[Int],
                                                 Browser: Option[Int],
                                                 InternetConnectionType: Option[Int],
                                                 MatchedFoldPosition: Int,

                                                 HasContextualCategoryTier1: Int,
                                                 ContextualCategoryLengthTier1: Float,
                                                 ContextualCategoriesTier1: Array[Int],

                                                 sin_hour_week: Float,
                                                 cos_hour_week: Float,
                                                 sin_hour_day: Float,
                                                 cos_hour_day: Float,
                                                 sin_minute_hour: Float,
                                                 cos_minute_hour: Float,
                                                 latitude: Option[Float],
                                                 longitude: Option[Float],

                                                 IndustryCategoryId: Option[Int],
                                                 AudienceId: Array[Int],

                                                 UserData: Array[Int],
                                                // user hotcache x audience segments
                                                 UserTargetingDataIds: Array[Int],
                                                 HasUserData: Int,
                                                 UserDataLength: Float,
                                                 UserDataOptIn: Int,
                                                 IdType: Int,
                                                 IdCount: Int,
                                                 UserAgeInDays: Option[Float]

                                               )

final case class DataForModelTrainingRecord(
                                 AdGroupIdStr: String,
                                 AdGroupId: Int,
                                 CampaignIdStr: String,
                                 CampaignId: Int,
                                 AdvertiserIdStr: String,
                                 AdvertiserId: Int,
                                 BidRequestIdStr: String,
                                 Weight: Float,
                                 Target: Float,
                                 Revenue: Option[Float],

                                 SupplyVendor: Option[Int],
                                 SupplyVendorPublisherId: Option[Int],
                                 AliasedSupplyPublisherId: Option[Int],
//                                 ImpressionPlacementId: Option[String],
                                 Site: Option[Int],
                                 AdFormat: Int,

                                 Country: Option[Int],
                                 Region: Option[Int],
                                 City: Option[Int],
                                 Zip: Option[Int],

                                 DeviceMake: Option[Int],
                                 DeviceModel: Option[Int],
                                 RequestLanguages: Int,

                                 RenderingContext: Option[Int],
                                 DeviceType: Option[Int],
                                 OperatingSystem: Option[Int],
                                 Browser: Option[Int],
                                 InternetConnectionType: Option[Int],
                                 MatchedFoldPosition: Int,

                                 HasContextualCategoryTier1: Int,
                                 ContextualCategoryLengthTier1: Float,

                                 ContextualCategoriesTier1_Column0: Int,
                                 ContextualCategoriesTier1_Column1: Int,
                                 ContextualCategoriesTier1_Column2: Int,
                                 ContextualCategoriesTier1_Column3: Int,
                                 ContextualCategoriesTier1_Column4: Int,
                                 ContextualCategoriesTier1_Column5: Int,
                                 ContextualCategoriesTier1_Column6: Int,
                                 ContextualCategoriesTier1_Column7: Int,
                                 ContextualCategoriesTier1_Column8: Int,
                                 ContextualCategoriesTier1_Column9: Int,
                                 ContextualCategoriesTier1_Column10: Int,
                                 ContextualCategoriesTier1_Column11: Int,
                                 ContextualCategoriesTier1_Column12: Int,
                                 ContextualCategoriesTier1_Column13: Int,
                                 ContextualCategoriesTier1_Column14: Int,
                                 ContextualCategoriesTier1_Column15: Int,
                                 ContextualCategoriesTier1_Column16: Int,
                                 ContextualCategoriesTier1_Column17: Int,
                                 ContextualCategoriesTier1_Column18: Int,
                                 ContextualCategoriesTier1_Column19: Int,
                                 ContextualCategoriesTier1_Column20: Int,
                                 ContextualCategoriesTier1_Column21: Int,
                                 ContextualCategoriesTier1_Column22: Int,
                                 ContextualCategoriesTier1_Column23: Int,
                                 ContextualCategoriesTier1_Column24: Int,
                                 ContextualCategoriesTier1_Column25: Int,
                                 ContextualCategoriesTier1_Column26: Int,
                                 ContextualCategoriesTier1_Column27: Int,
                                 ContextualCategoriesTier1_Column28: Int,
                                 ContextualCategoriesTier1_Column29: Int,
                                 ContextualCategoriesTier1_Column30: Int,

                                 sin_hour_week: Float,
                                 cos_hour_week: Float,
                                 sin_hour_day: Float,
                                 cos_hour_day: Float,
                                 sin_minute_hour: Float,
                                 cos_minute_hour: Float,
                                 latitude: Option[Float],
                                 longitude: Option[Float],

                                 IndustryCategoryId: Option[Int],
                                 AudienceId_Column0: Int,
                                 AudienceId_Column1: Int,
                                 AudienceId_Column2: Int,
                                 AudienceId_Column3: Int,
                                 AudienceId_Column4: Int,
                                 AudienceId_Column5: Int,
                                 AudienceId_Column6: Int,
                                 AudienceId_Column7: Int,
                                 AudienceId_Column8: Int,
                                 AudienceId_Column9: Int,
                                 AudienceId_Column10: Int,
                                 AudienceId_Column11: Int,
                                 AudienceId_Column12: Int,
                                 AudienceId_Column13: Int,
                                 AudienceId_Column14: Int,
                                 AudienceId_Column15: Int,
                                 AudienceId_Column16: Int,
                                 AudienceId_Column17: Int,
                                 AudienceId_Column18: Int,
                                 AudienceId_Column19: Int,
                                 AudienceId_Column20: Int,
                                 AudienceId_Column21: Int,
                                 AudienceId_Column22: Int,
                                 AudienceId_Column23: Int,
                                 AudienceId_Column24: Int,
                                 AudienceId_Column25: Int,
                                 AudienceId_Column26: Int,
                                 AudienceId_Column27: Int,
                                 AudienceId_Column28: Int,
                                 AudienceId_Column29: Int
                                          )

case class DataCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[DataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset/csv/v=2",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class DataCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[DataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_click/csv/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class DataIncCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[DataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_inc/csv/v=2",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class DataIncCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[DataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_click_inc/csv/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class UserDataIncCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[UserDataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_userdata_inc/csv/v=2",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class UserDataCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[UserDataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_userdata/csv/v=2",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class UserDataCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[UserDataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_click_userdata/csv/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class UserDataIncCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[UserDataForModelTrainingRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/trainset_click_userdata_inc/csv/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class ArrayUserDataIncCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3CBufferDataset[ArrayUserDataForModelTrainingRecord](
    MLPlatformS3Root, s"${BaseFolderPath}/trainset_userdata_inc/cb/v=1", Some("split"), experimentOverride = experimentOverride) {
}

case class ArrayUserDataCsvForModelTrainingDatasetLastTouch(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3CBufferDataset[ArrayUserDataForModelTrainingRecord](
    MLPlatformS3Root, s"${BaseFolderPath}/trainset_userdata/cb/v=1", Some("split"), experimentOverride = experimentOverride) {
}

case class ArrayUserDataCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3CBufferDataset[ArrayUserDataForModelTrainingRecord](
    MLPlatformS3Root, s"${BaseFolderPath}/trainset_click_userdata/cb/v=1", Some("split"), experimentOverride = experimentOverride) {
}

case class ArrayUserDataIncCsvForModelTrainingDatasetClick(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3CBufferDataset[ArrayUserDataForModelTrainingRecord](
    MLPlatformS3Root, s"${BaseFolderPath}/trainset_click_userdata_inc/cb/v=1", Some("split"), experimentOverride = experimentOverride) {
}
