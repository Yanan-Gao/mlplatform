package com.thetradedesk.philo.schema

case class AdGroupFilterRecord(
    AdGroupId: String
)

object AdGroupFilterDataset {
  val AGFILTERS3 = (env: String) =>  f"s3://thetradedesk-mlplatform-us-east-1/data/${env}/philo/adgroupfilter/"
}
