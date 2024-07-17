package com.thetradedesk.plutus.data.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, LocalDateTime}

class packageTest extends AnyFlatSpec {


  "jodatojava" should "be equivalent to javatojoda" in {
    val now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)
    val jodaDateTime = javaToJoda(now)
    val javaDateTime = jodaToJava(jodaDateTime)

    assert(now == javaDateTime)
  }
}

