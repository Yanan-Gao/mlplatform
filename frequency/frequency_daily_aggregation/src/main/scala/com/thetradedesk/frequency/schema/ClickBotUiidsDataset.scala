package com.thetradedesk.frequency.schema

/** UIID list flagged as potential click bots. */
case class ClickBotUiidRecord(UIID: String)

object ClickBotUiidsDataSet {
  // Base output root for click-bot UIID list
  val ROOT_TEMPLATE: String = "s3://thetradedesk-mlplatform-us-east-1/env={env}/features/data/frequency/click_bot_uiids/v1"
  def root(env: String): String = ROOT_TEMPLATE.replace("{env}", env)
}

