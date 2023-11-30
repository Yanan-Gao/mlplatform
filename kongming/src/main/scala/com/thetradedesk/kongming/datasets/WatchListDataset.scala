package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.Csv

final case class WatchListRecord(
                                  Level: String,
                                  Id: String,
                                )

case class WatchListDataset(experimentOverride: Option[String] = None) extends KongMingDataset[WatchListRecord](
  s3DatasetPath = "watchlist/v=1",
  experimentOverride = experimentOverride,
  fileFormat = Csv.WithHeader
)