package com.thetradedesk.philo.schema

case class PartnerExclusionRecord(PartnerId: String)
object PartnerExclusionList {
  val PARTNEREXCLUSIONS3: String = "s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/philo/partnerFilter/partner_exclusion_list.csv"
}
