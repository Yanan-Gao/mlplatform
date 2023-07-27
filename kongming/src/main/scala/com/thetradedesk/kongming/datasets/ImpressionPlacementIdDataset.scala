package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.Tsv
import com.thetradedesk.spark.util.TTDConfig.config

final case class ImpressionPlacementIdSchema(
                                              ImpressionPlacementId: String
                                          )

case class ImpressionPlacementIdDataset(experimentName: String = "") extends KongMingDataset[ImpressionPlacementIdSchema](
  s3DatasetPath = "modelassets/impressionplacementid/v=1",
  experimentName = config.getString("ttd.ImpressionPlacementIdDataset.experimentName", experimentName),
  fileFormat = Tsv.Headerless
)