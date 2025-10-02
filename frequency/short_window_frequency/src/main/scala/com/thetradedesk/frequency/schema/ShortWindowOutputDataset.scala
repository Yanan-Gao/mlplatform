package com.thetradedesk.frequency.schema

import java.sql.Date

// Default short-window output schema for windows: 1hr, 3hr, 6hr, 12hr
case class ShortWindowRecord(
  BidRequestId: String,
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
  impression_time_unix: Long,
  date: Date,
  label: Int,
  // same-campaign window counts
  campaign_impression_count_1hr: Long,
  campaign_impression_count_3hr: Long,
  campaign_impression_count_6hr: Long,
  campaign_impression_count_12hr: Long,
  campaign_click_count_1hr: Long,
  campaign_click_count_3hr: Long,
  campaign_click_count_6hr: Long,
  campaign_click_count_12hr: Long,
  // cross-campaign (user) window counts
  user_impression_count_1hr: Long,
  user_impression_count_3hr: Long,
  user_impression_count_6hr: Long,
  user_impression_count_12hr: Long,
  user_click_count_1hr: Long,
  user_click_count_3hr: Long,
  user_click_count_6hr: Long,
  user_click_count_12hr: Long,
  // same-day counts
  campaign_impression_count_same_day: Long,
  campaign_click_count_same_day: Long,
  user_impression_count_same_day: Long,
  user_click_count_same_day: Long,
)

object ShortWindowOutputDataSet {
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/short_window/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}
