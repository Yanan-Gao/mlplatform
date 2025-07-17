package com.thetradedesk.plutus.data.schema.monitoring
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.sql.Date
import java.time.LocalDate

case class BackoffMonitoringSchema(
                                    Date: Date,
                                    CampaignId:String,
                                    Bids:Long,
                                    Actual_BBF_OptOut_Bids:Long,
                                    Sim_BBF_OptOut_Rate:Double,
                                    Sim_BBF_PMP_OptOut_Rate:Double,
                                    Sim_UnderdeliveryFraction:Double,
                                    CampaignBbfFloorBuffer:Double,
                                    CampaignPCAdjustment:Double,
                                    Actual_BBF_OptOut_Rate:Double,
                                    MaxBidMultiplierCap: Double,
                                    TestBucket:Short,
                                    BucketGroup:String
                                 )

object BackoffMonitoringDataset extends S3DailyParquetDataset[BackoffMonitoringSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusdashboard/hadesbackoffmonitoring/v=${DATA_VERSION}"
  }
}
