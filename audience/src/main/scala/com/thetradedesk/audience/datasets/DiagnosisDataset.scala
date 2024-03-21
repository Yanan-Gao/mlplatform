package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.ttdEnv

final case class DiagnosisRecord(CampaignId: String,
                                 AdGroupId: String,
                                 ROIGoalTypeId: BigInt,
                                 ReportHourUtc: String,
                                 TotalCount: BigInt,
                                 CountMetrics: String
                                )

case class DiagnosisDataset() extends LightWritableDataset[DiagnosisRecord](s"${ttdEnv}/distributedalgodiagnosis/v=1", ML_PLATFORM_ROOT, 1)
