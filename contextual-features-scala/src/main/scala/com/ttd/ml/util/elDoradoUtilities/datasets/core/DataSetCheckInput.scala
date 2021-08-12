//package com.ttd.ml.util.elDoradoUtilities.datasets.core
/** Note: This is in el-dorado but commented out here to remove dependency on com.amazon.deequ */

//import com.amazon.deequ.AnomalyCheckConfig
//import com.amazon.deequ.anomalydetection.{AnomalyDetectionStrategy, RelativeRateOfChangeStrategy}
//import com.amazon.deequ.checks.{CheckLevel, CheckWithLastConstraintFilterable}
//
//case class AnomalyCheck(strategy: AnomalyDetectionStrategy, analyzer: String, columnName: String = null, config: AnomalyCheckConfig) {}
//
//case class DataSetCheckInput(isCompleteColumns: List[String],
//                             isNonNegativeColumns: List[String],
//                             isContainedInColumns: List[(String, Array[String])],
//                             checkMessage: String,
//                             sanityErrorCheck: CheckWithLastConstraintFilterable,
//                             sanityWarningCheck: CheckWithLastConstraintFilterable,
//                             metricsRepositoryPath: String,
//                             anomalyChecks: List[AnomalyCheck]
//                            ) {}
//
//object DataSetCheckInput {
//  // strategy that compares metrics through percentage of change, i.e. checks if size decreases over 90%
//  val relativeRateOfChangeStrategy: RelativeRateOfChangeStrategy = RelativeRateOfChangeStrategy(maxRateDecrease = Some(0.1))
//
//  // config for the size score anomaly check
//  val sizeScoreAnomalyCheckConfig: AnomalyCheckConfig = AnomalyCheckConfig(
//    CheckLevel.Error,
//    "Avoid cases when the pipeline failed to collect the data, thus size cannot be 90% lower than min value in the last month"
//  )
//
//  // config for the completeness final score and z score anomaly check
//  val completenessScoreAnomalyCheckConfig: AnomalyCheckConfig = AnomalyCheckConfig(
//    CheckLevel.Warning,
//    "All metrics should have a minimum coverage associated with it to detect when there are too many null entries"
//  )
//
//  // config for the mean final score anomaly check
//  val meanFinalScoreAnomalyCheckConfig: AnomalyCheckConfig = AnomalyCheckConfig(
//    CheckLevel.Warning,
//    "Detects anomalies based on the running mean within 3 standard deviations"
//  )
//}

