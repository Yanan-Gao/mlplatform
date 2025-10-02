package com.thetradedesk.frequency.schema

object DailyAggregateDataSet {
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/daily_agg/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}

