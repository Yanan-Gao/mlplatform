package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import java.sql.Timestamp

case class ROIGoalTypeRecord(ROIGoalTypeId: Int,
                             ROIGoalTypeName: String)

case class ROIGoalTypeDataset() extends
  ProvisioningS3DataSet[ROIGoalTypeRecord]("roigoaltype/v=1")