package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName, audienceVersionDateFormat, audienceResultCoalesce}


final case class TrainingDataStatusRecord(SyntheticId: Int,
                                      Density1PosRatio: Option[Double],
                                      Density2PosRatio: Option[Double],
                                      Density3PosRatio: Option[Double],
                                      weighted_pos_ratio: Double,
                                      avgPosRatio: Double,
                                      percentile25: Double,
                                      percentile50: Double,
                                      percentile75: Double,
                                      DataType: String)

case class TrainingDataStatusDataSet() extends LightWritableDataset[TrainingDataStatusRecord](s"/${ttdEnv}/audience/RSMV2/measurement/trainingDataStatus/v=1", ML_PLATFORM_ROOT, 128, dateFormat = audienceVersionDateFormat.substring(0, 8))


final case class TrainingMetaDataRecord(SyntheticId: Long,
                                      posRatio: Double,
                                      cnt: Long,
                                      ActiveSize: Long,
                                      Density1PosRatio: Option[Double],
                                      Density2PosRatio: Option[Double],
                                      Density3PosRatio: Option[Double],
                                      DataType: String)

case class TrainingMetaDataSet() extends LightWritableDataset[TrainingMetaDataRecord](s"/${ttdEnv}/audience/RSMV2/measurement/trainingMetaData/v=1", ML_PLATFORM_ROOT, 128, dateFormat = audienceVersionDateFormat.substring(0, 8))