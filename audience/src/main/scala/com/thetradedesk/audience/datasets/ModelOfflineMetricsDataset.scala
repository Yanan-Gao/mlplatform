package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName, audienceVersionDateFormat, audienceResultCoalesce, ttdWriteEnv}
import java.sql.Date
import com.thetradedesk.spark.util.TTDConfig.config



final case class ModelOfflineMetricsRecord(
        PopulationImpCount: Option[Long],
        PopulationAUC: Option[Double],
        PopulationPredStddev: Option[Double],
        PopulationPosRatio: Option[Double],
        PopulationInSeedAvgPred: Option[Double],
        PopulationOutSeedAvgPred: Option[Double],
        PopulationDensity1PosRatio: Option[Double],
        PopulationDensity2PosRatio: Option[Double],
        PopulationDensity3PosRatio: Option[Double],
        PopulationPrecision: Option[Double],
        PopulationRecall: Option[Double],

        OOSImpCount: Option[Long],
        OOSAUC: Option[Double],
        OOSPredStddev: Option[Double],
        OOSPosRatio: Option[Double],
        OOSInSeedAvgPred: Option[Double],
        OOSOutSeedAvgPred: Option[Double],
        OOSDensity1PosRatio: Option[Double],
        OOSDensity2PosRatio: Option[Double],
        OOSDensity3PosRatio: Option[Double],
        OOSPrecision: Option[Double],
        OOSRecall: Option[Double],
        
        SyntheticId: Long, 

)

case class ModelOfflineMetricsDataset(model: String, tag: String, version: Int = 1) extends LightWritableDataset[ModelOfflineMetricsRecord](s"${ttdWriteEnv}/audience/${model}/${tag}/v=${version}", ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)

