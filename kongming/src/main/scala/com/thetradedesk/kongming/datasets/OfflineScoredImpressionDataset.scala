package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.getExperimentPath
import com.thetradedesk.spark.util.TTDConfig.config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

final case class OfflineScoredImpressionRecord(
                                              BidRequestId: String,
                                              BaseAdGroupId: String,
                                              AdGroupId: String,
                                              Score: Double
                                              )


case class OfflineScoredImpressionDataset(modelDate: LocalDate) extends KongMingDataset[OfflineScoredImpressionRecord](
  s3DatasetPath = s"${getExperimentPath(config.getString("ttd.DailyOfflineScoringDataset.experimentName", ""))}measurement/offline/v=1/model_date=${modelDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}",
  partitionField = "scored_date"
)
