package com.thetradedesk.philo.schema

case class AdvertiserExclusionRecord(AdvertiserId: String)
object AdvertiserExclusionList {
  val ADVERTISEREXCLUSIONS3: String = "s3://thetradedesk-mlplatform-us-east-1/metadata/philo/prod/advertiserFilter/advertiser_exclusion_list.csv"
}
