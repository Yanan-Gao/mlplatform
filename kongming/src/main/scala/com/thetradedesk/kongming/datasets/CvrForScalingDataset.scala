package com.thetradedesk.kongming.datasets

final case class CvrForScalingRecord(
                                              Level: String,
                                              Id: String,
                                              CVR: Double,
                                              CVRSmooth: Double,
                                              LastUpdateDate: String,
                                            )

case class CvrForScalingDataset(experimentOverride: Option[String] = None) extends KongMingDataset[CvrForScalingRecord](
  s3DatasetPath = "calibration/cvrforscaling/v=1",
  experimentOverride = experimentOverride
 ) {
  override protected def getMetastoreTableName: String = "cvrforscaling"
}
