package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import java.sql.Timestamp

/**
 * A provisioning dataset of {@link CampaignFlightRecord}
 */
case class CampaignFlightDataSet() extends
  ProvisioningS3DataSet[CampaignFlightRecord]("campaignflight/v=1")

/**
 * A record that mirrors a Provisioning database's CampaignFlight row.
 *
 * @param CampaignFlightId                unique id of the campaign flight
 * @param CampaignId                      the campaign id
 * @param IsDeleted                       whether the flight is deleted
 * @param StartDateInclusiveUTC           start date of the campaign in UTC timezone
 * @param EndDateExclusiveUTC             end date of the campaign in UTC timezone
 * @param BudgetInAdvertiserCurrency      budget of the flight in dollar values
 * @param BudgetInImpressions             budget of the flight in impressions
 * @param DailyTargetInAdvertiserCurrency daily target for this flight in dollar values
 * @param DailyTargetInImpressions        daily target for this flight in impressions
 */
case class CampaignFlightRecord(CampaignFlightId: BigInt,
                                CampaignId: String,
                                IsDeleted: Boolean,
                                StartDateInclusiveUTC: Timestamp,
                                EndDateExclusiveUTC: Timestamp,
                                BudgetInAdvertiserCurrency: BigDecimal,
                                BudgetInImpressions: BigInt,
                                DailyTargetInAdvertiserCurrency: BigDecimal,
                                DailyTargetInImpressions: BigInt)
