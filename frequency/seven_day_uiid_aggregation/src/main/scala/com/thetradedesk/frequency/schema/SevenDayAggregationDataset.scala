package com.thetradedesk.frequency.schema

object SevenDayAggregationDataSet {
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/seven_day_agg/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}

