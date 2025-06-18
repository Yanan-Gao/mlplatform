package com.thetradedesk.featurestore.utils

import org.scalatest.funsuite.AnyFunSuite

class StringUtilsTest extends AnyFunSuite {

  test("applyNamedFormat should replace the placeholders by value map") {
    val instanceString = "Hello, name {name} {name}! Welcome to {city} city."
    val overridesMap = Map("name" -> "Alice", "city" -> "Wonderland")
    val res = StringUtils.applyNamedFormat(instance = instanceString, overrides = overridesMap)
    assertResult("Hello, name Alice Alice! Welcome to Wonderland city.")(res)
  }

}
