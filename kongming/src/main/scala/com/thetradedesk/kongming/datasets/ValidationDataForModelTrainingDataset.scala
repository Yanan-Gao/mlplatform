package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{MLPlatformS3Root, getExperimentPath}
import com.thetradedesk.spark.datasets.core._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

final case class ValidationDataForModelTrainingRecord(BidRequestIdStr: String,
                                                      AdGroupIdStr: String,
                                                      AdGroupId: Int,
                                                      Weight: Double,
                                                      Target: Int,

                                                      SupplyVendor: Option[Int],
                                                      SupplyVendorPublisherId: Option[Int],
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

                                                      sin_hour_week: Double,
                                                      cos_hour_week: Double,
                                                      sin_hour_day: Double,
                                                      cos_hour_day: Double,
                                                      sin_minute_hour: Double,
                                                      cos_minute_hour: Double,
                                                      Latitude: Option[Double],
                                                      Longitude: Option[Double]
                                                     )

/**
 * This data set is very close to DataForModelTrainingDataset. It stores  saves data with Parquet.
 * It will be used for various testing purposes since TF record isn't widely used.
 */
case class ValidationDataForModelTrainingDataset(experimentName: String = "")
  extends PartitionedS3DataSet2[ValidationDataForModelTrainingRecord, LocalDate, String, String, String](
    GeneratedDataSet, MLPlatformS3Root, s"kongming/${getExperimentPath(experimentName)}trainset/parquet/v=1",
    "date" -> ColumnExistsInDataSet,
    "split" -> ColumnExistsInDataSet,
    fileFormat = Parquet,
    writeThroughHdfs = true
  ) {

  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet
  def partitionField2: (String, PartitionColumnCalculation) = "split" -> ColumnExistsInDataSet

  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  override def toStoragePartition1(date: LocalDate): String = date.format(dateTimeFormat)
  override def toStoragePartition2(split: String): String = split
}
