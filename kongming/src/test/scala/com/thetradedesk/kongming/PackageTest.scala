package com.thetradedesk.kongming

import org.scalatest.funsuite.AnyFunSuite

class PackageTest extends AnyFunSuite {
  test("encodeStringIds results match BidderCache") {
    assertResult(3922088881324l)(encodeStringId("gmihi3t"))
  }
}
