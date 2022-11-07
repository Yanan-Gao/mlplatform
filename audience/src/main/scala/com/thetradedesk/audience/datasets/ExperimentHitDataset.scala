package com.thetradedesk.audience.datasets

final case class ExperimentHitRecord(LogTime: java.sql.Timestamp,
                                     AvailableBidRequestId: String,
                                    TDID: String,
                                    AdGroupId: String,
                                    AEScore: Double,
                                    Type: Int
                                    )

case class ExperimentHitDataset() extends
  LightReadableDataset[ExperimentHitRecord]("audienceextensionalphaabtest/collected", S3Roots.LOGS_ROOT, source = Some(DatasetSource.Logs))

object ExperimentHitType extends Enumeration {
  type Type = Value

  val LAL, Model = Value
}