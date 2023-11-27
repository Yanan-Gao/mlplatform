package com.thetradedesk.philo.schema

case class CreativeLandingPageRecord(CreativeId: String, CreativeLandingPageId: String)
object CreativeLandingPageDataset {
  val CREATIVELANDINGPAGES3: String = "s3://thetradedesk-mlplatform-us-east-1/users/jiaxing.pi/CreativeLandingPage.csv" // TODO add real path after export job done
}
