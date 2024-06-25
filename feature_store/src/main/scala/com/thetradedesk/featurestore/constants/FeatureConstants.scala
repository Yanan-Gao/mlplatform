package com.thetradedesk.featurestore.constants

object FeatureConstants {
  val UserIDKey: String = "TDID"
  val FeatureDataKey: String = "data"
  val AlphaNumericRegex: String = "[A-Za-z0-9]+"
  val MaxArrayLength = 255
  val BitsOfByte: Int = 8

  val WIDTH_8 = 0
  val WIDTH_16 = 1
  val WIDTH_32 = 2
  val WIDTH_64 = 3

  val BytesToKeepAddress = 2

  val DefaultMaxDataSizePerRecord = 1024
  val UserFeatureDataPartitionNumbers = 4096
  val DefaultMaxRecordsPerFile = 500000
  val SchemaFileName = "schema.json"

  val ML_PLATFORM_S3_PATH = "s3a://thetradedesk-mlplatform-us-east-1"

  val SecondsPerHour: Int = 3600
  val SecondsPerDay: Int = SecondsPerHour * 24

  val ROIGoalTypeId_CPA = 5
  val ROIGoalTypeId_ROAS = 6
  val AttributedEventTypeId_Click = "1"
  val AttributedEventTypeId_View = "2"
  val AttributionMethodId_LastClick = "0"
  val AttributionMethodId_ViewThru = "1"



}