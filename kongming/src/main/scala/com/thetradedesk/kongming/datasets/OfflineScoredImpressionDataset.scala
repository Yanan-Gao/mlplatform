package com.thetradedesk.kongming.datasets

import java.time.LocalDate
import java.time.format.DateTimeFormatter

final case class OfflineScoredImpressionRecord(
                                              BidRequestId: String,
                                              BaseAdGroupId: String,
                                              AdGroupId: String,
                                              Score: Array[Double]
                                              )


case class OfflineScoredImpressionDataset(modelDate: LocalDate) extends KongMingDataset[OfflineScoredImpressionRecord](
  s3DatasetPath = s"measurement/offline/v=1/date=${modelDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}",
  partitionField = "scored_date"
)
