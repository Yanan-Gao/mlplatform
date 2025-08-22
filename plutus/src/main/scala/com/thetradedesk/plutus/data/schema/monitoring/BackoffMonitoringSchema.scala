package com.thetradedesk.plutus.data.schema.monitoring
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.sql.Date
import java.time.LocalDate

case class BackoffMonitoringSchema(
                                    Date: Date,
                                    CampaignId: String,
                                    IsValuePacing: Boolean,
                                    TestBucket: Short,
                                    BucketGroup: String,
                                    IsProgrammaticGuaranteed: Option[String],
                                    IsBaseBidOptimized: Option[String],
                                    CampaignPredictiveClearingEnabled: Option[Boolean],
                                    PCBid_Rate: Option[Double],
                                    PCSpend_Rate: Option[Double],
                                    CampaignBbfFloorBuffer: Option[Double],
                                    CampaignPCAdjustment: Option[Double],
                                    VirtualMaxBid_Multiplier: Option[Double],
                                    MaxBidMultiplierCap: Option[Double],

                                    // Campaign Throttle Dataset Metrics
                                    UnderdeliveryFraction: Option[Double],
                                    EstimatedBudgetInUSD: Option[Double],
                                    TotalAdvertiserCostFromPerformanceReportInUSD: Option[Double],

                                    // Bids, Fees & Spend from PcResultsGeronimo
                                    InitialBid: Option[Double],
                                    MaxBidCpmInBucks: Option[Double],
                                    InternalBidOverMaxBid_Rate: Option[Double],
                                    FeeAmount: Option[Double],
                                    PartnerCostInUSD: Option[Double],
                                    PCFeesOverSpend_Rate: Option[Double],

                                    // HadesBackoff Simulated OptOut Rates
                                    Sim_BBF_OptOut_Rate: Option[Double],
                                    Sim_BBF_PMP_OptOut_Rate: Option[Double],
                                    Sim_BBF_OM_OptOut_Rate: Option[Double],

                                    // Calculated OptOut Rates
                                    BBF_OptOut_Rate: Option[Double],
                                    BBF_PMP_OptOut_Rate: Option[Double],
                                    BBF_OM_OptOut_Rate: Option[Double],
                                    NonBBF_OptOut_Rate: Option[Double],

                                    // Bid Counts
                                    Bids: Option[Long],
                                    PMP_Bids: Option[Long],
                                    OM_Bids: Option[Long],
                                    BBF_OptOut_Bids: Option[Long],
                                    BBF_PMP_OptOut_Bids: Option[Long],
                                    BBF_OM_OptOut_Bids: Option[Long],
                                    NonBBF_OptOut_Bids: Option[Long]

                                    // Campaign performance status
//                                    PacingStatus: String,
//                                    InternalBidOverMaxBidStatus: String,
//                                    PCFeesStatus: String,
//                                    ProblemStatus: String,
//                                    HighUnderdeliveryFraction_Threshold: Option[Double],
//                                    HighInternalBidOverMaxBid_Rate_Threshold: Option[Double],
//                                    HighPCFeesOverSpend_Rate_Threshold: Option[Double]
                                 )

object BackoffMonitoringDataset extends S3DailyParquetDataset[BackoffMonitoringSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusdashboard/hadesbackoffmonitoring/v=${DATA_VERSION}"
  }
}
