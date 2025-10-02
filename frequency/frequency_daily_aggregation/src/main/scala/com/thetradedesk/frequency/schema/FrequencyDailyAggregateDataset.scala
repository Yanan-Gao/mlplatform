package com.thetradedesk.frequency.schema

case class FrequencyDailyAggregateRecord(
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
  IndustryCategoryId: Option[String],
  date: String,
  impression_count: Long,
  click_count: Long
)

object FrequencyDailyAggregateDataSet {
  // Base output root for daily aggregation outputs
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/daily_agg/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}
