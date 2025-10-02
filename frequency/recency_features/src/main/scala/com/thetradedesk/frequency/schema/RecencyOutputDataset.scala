package com.thetradedesk.frequency.schema

import java.sql.Date

case class RecencyRecord(
  BidRequestId: String,
  UIID: String,
  CampaignId: String,
  AdvertiserId: String,
  impression_time_unix: Long,
  date: Date,
  label: Int,
  uiid_campaign_recency_impression_seconds: Long,
  uiid_campaign_recency_click_seconds: Long,
  uiid_recency_impression_seconds: Long,
  uiid_recency_click_seconds: Long
)

object RecencyOutputDataSet {
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/recency/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}

