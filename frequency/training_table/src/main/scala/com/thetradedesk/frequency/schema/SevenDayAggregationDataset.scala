package com.thetradedesk.frequency.schema

case class SevenDayUiidCampaignRecord(
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
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
  impression_count_offsets_1_2: Long,
  click_count_offsets_1_2: Long,
  impression_count_offsets_1_2_3_4: Long,
  click_count_offsets_1_2_3_4: Long,
  impression_count_offsets_1_2_3_4_5_6: Long,
  click_count_offsets_1_2_3_4_5_6: Long
)

case class SevenDayPerUiidRecord(
  UIID: String,
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
  uiid_impression_count_offsets_1_2: Long,
  uiid_click_count_offsets_1_2: Long,
  uiid_impression_count_offsets_1_2_3_4: Long,
  uiid_click_count_offsets_1_2_3_4: Long,
  uiid_impression_count_offsets_1_2_3_4_5_6: Long,
  uiid_click_count_offsets_1_2_3_4_5_6: Long
)

object SevenDayAggregationDataSet {
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/seven_day_agg/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}
