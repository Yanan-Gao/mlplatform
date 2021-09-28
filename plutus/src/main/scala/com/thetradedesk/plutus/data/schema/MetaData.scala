package com.thetradedesk.plutus.data.schema

import java.time.LocalDate

case class MetaData(date: LocalDate, train: Long, test: Long, validation: Long)
