package com.thetradedesk.plutus.data.schema.monitoring
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.sql.Date
import java.time.LocalDate

case class BackoffMonitoringSchema(
                                    Date: Date,
                                    CampaignId:String,
                                    IsValuePacing: Boolean,
                                    TestBucket:Short,
                                    BucketGroup:String,
                                    CampaignBbfFloorBuffer:Option[Double],
                                    CampaignPCAdjustment:Option[Double],
                                    MaxBidMultiplierCap: Option[Double],
                                    UnderdeliveryFraction:Option[Double],

                                    // HadesBackoff Simulated OptOut Rates
                                    Sim_BBF_OptOut_Rate:Option[Double],
                                    Sim_BBF_PMP_OptOut_Rate:Option[Double],
                                    Sim_BBF_OM_OptOut_Rate:Option[Double],

                                    // Calculated OptOut Rates
                                    BBF_OptOut_Rate:Option[Double],
                                    BBF_PMP_OptOut_Rate: Option[Double],
                                    BBF_OM_OptOut_Rate: Option[Double],
                                    NonBBF_OptOut_Rate:Option[Double],

                                    // Bid Counts
                                    Bids:Option[Long],
                                    PMP_Bids:Option[Long],
                                    OM_Bids:Option[Long],
                                    BBF_OptOut_Bids:Option[Long],
                                    BBF_PMP_OptOut_Bids:Option[Long],
                                    BBF_OM_OptOut_Bids:Option[Long],
                                    NonBBF_OptOut_Bids: Option[Long]
                                 )

object BackoffMonitoringDataset extends S3DailyParquetDataset[BackoffMonitoringSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusdashboard/hadesbackoffmonitoring/v=${DATA_VERSION}"
  }
}
