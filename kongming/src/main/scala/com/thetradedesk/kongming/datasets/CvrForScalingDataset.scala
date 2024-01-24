package com.thetradedesk.kongming.datasets

final case class CvrForScalingRecord(
                                              Level: String,
                                              Id: String,
                                              CVR: Double,
                                            )

case class CvrForScalingDataset(experimentOverride: Option[String] = None) extends KongMingDataset[CvrForScalingRecord](
  s3DatasetPath = "cvrforscaling/v=1",
  experimentOverride = experimentOverride
)
