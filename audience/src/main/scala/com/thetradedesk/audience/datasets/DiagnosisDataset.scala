package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.ttdEnv

import java.sql.Timestamp

final case class DiagnosisRecord(CampaignId: String,
                                 AdGroupId: String,
                                 ROIGoalTypeId: BigInt,
                                 ReportHourUtc: Timestamp,
                                 TotalCount: BigInt,
                                 CountMetrics: String
                                )

case class DiagnosisWritableDataset() extends LightWritableDataset[DiagnosisRecord](s"${ttdEnv}/distributedalgodiagnosis/v=1", ML_PLATFORM_ROOT, 1)

case class
DiagnosisReadableDataset() extends LightReadableDataset[DiagnosisRecord](s"${ttdEnv}/distributedalgodiagnosis/v=1", ML_PLATFORM_ROOT)
