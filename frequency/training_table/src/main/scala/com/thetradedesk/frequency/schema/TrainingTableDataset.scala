package com.thetradedesk.frequency.schema

import java.sql.Date

// Default aggregate sets: [[1,2],[1,2,3,4],[1,2,3,4,5,6]] => approx windows for 2d, 4d, 6d
// Short-window defaults: 1hr, 3hr, 6hr, 12hr

case class TrainingTableRecord(
  BidRequestId: String,
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
  impression_time_unix: Long,
  date: Date,
  label: Int,
  // short-window campaign
  campaign_impression_count_1hr: Long,
  campaign_impression_count_3hr: Long,
  campaign_impression_count_6hr: Long,
  campaign_impression_count_12hr: Long,
  campaign_click_count_1hr: Long,
  campaign_click_count_3hr: Long,
  campaign_click_count_6hr: Long,
  campaign_click_count_12hr: Long,
  // short-window user
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
  // seven-day UIID+Campaign day counts
  impression_count_d1: Long,
  impression_count_d2: Long,
  impression_count_d3: Long,
  impression_count_d4: Long,
  impression_count_d5: Long,
  impression_count_d6: Long,
  impression_count_d7: Long,
  click_count_d1: Long,
  click_count_d2: Long,
  click_count_d3: Long,
  click_count_d4: Long,
  click_count_d5: Long,
  click_count_d6: Long,
  click_count_d7: Long,
  // seven-day UIID day counts
  uiid_impression_count_d1: Long,
  uiid_impression_count_d2: Long,
  uiid_impression_count_d3: Long,
  uiid_impression_count_d4: Long,
  uiid_impression_count_d5: Long,
  uiid_impression_count_d6: Long,
  uiid_impression_count_d7: Long,
  uiid_click_count_d1: Long,
  uiid_click_count_d2: Long,
  uiid_click_count_d3: Long,
  uiid_click_count_d4: Long,
  uiid_click_count_d5: Long,
  uiid_click_count_d6: Long,
  uiid_click_count_d7: Long,
  // seven-day aggregate sets (campaign)
  impression_count_offsets_1_2: Long,
  click_count_offsets_1_2: Long,
  impression_count_offsets_1_2_3_4: Long,
  click_count_offsets_1_2_3_4: Long,
  impression_count_offsets_1_2_3_4_5_6: Long,
  click_count_offsets_1_2_3_4_5_6: Long,
  // seven-day aggregate sets (uiid)
  uiid_impression_count_offsets_1_2: Long,
  uiid_click_count_offsets_1_2: Long,
  uiid_impression_count_offsets_1_2_3_4: Long,
  uiid_click_count_offsets_1_2_3_4: Long,
  uiid_impression_count_offsets_1_2_3_4_5_6: Long,
  uiid_click_count_offsets_1_2_3_4_5_6: Long,
  // recency columns
  uiid_campaign_recency_impression_seconds: Long,
  uiid_campaign_recency_click_seconds: Long,
  uiid_recency_impression_seconds: Long,
  uiid_recency_click_seconds: Long,
  // long-window approx outputs
  same_campaign_imps_2d_approx: Double,
  same_campaign_clicks_2d_approx: Double,
  same_campaign_imps_4d_approx: Double,
  same_campaign_clicks_4d_approx: Double,
  same_campaign_imps_6d_approx: Double,
  same_campaign_clicks_6d_approx: Double,
  cross_campaign_imps_2d_approx: Double,
  cross_campaign_clicks_2d_approx: Double,
  cross_campaign_imps_4d_approx: Double,
  cross_campaign_clicks_4d_approx: Double,
  cross_campaign_imps_6d_approx: Double,
  cross_campaign_clicks_6d_approx: Double,
  // day progress
  day_progress_fraction: Double
)

object TrainingTableDataSet { 
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/training_table/v1" 
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}

