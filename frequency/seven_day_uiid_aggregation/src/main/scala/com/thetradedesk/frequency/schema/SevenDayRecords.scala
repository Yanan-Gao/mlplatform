package com.thetradedesk.frequency.schema

// Default seven-day lookback and aggregate sets
// lookbackDays = 7
// aggregate_sets = [[1,2],[1,2,3,4],[1,2,3,4,5,6]]

/** UIID + Campaign schema for default seven-day outputs. */
case class SevenDayUiidCampaignRecord(
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
  // day-wise counts (1..7)
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
  // aggregate sets
  impression_count_offsets_1_2: Long,
  click_count_offsets_1_2: Long,
  impression_count_offsets_1_2_3_4: Long,
  click_count_offsets_1_2_3_4: Long,
  impression_count_offsets_1_2_3_4_5_6: Long,
  click_count_offsets_1_2_3_4_5_6: Long
)

/** UIID-only schema for default seven-day outputs. */
case class SevenDayPerUiidRecord(
  UIID: String,
  // day-wise counts (1..7)
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
  // aggregate sets
  uiid_impression_count_offsets_1_2: Long,
  uiid_click_count_offsets_1_2: Long,
  uiid_impression_count_offsets_1_2_3_4: Long,
  uiid_click_count_offsets_1_2_3_4: Long,
  uiid_impression_count_offsets_1_2_3_4_5_6: Long,
  uiid_click_count_offsets_1_2_3_4_5_6: Long
)

