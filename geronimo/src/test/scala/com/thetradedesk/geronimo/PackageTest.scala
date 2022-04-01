package com.thetradedesk.geronimo

import com.thetradedesk.geronimo.shared.shiftMod
import org.scalatest.flatspec.AnyFlatSpec

class packageTest extends AnyFlatSpec {

  "shiftMod" should "move the value up to allow unk at zero" in {
    val expected = 5959
    val sparkMagicSeedNumber = 42L
    val s = org.apache.spark.unsafe.types.UTF8String.fromString("ttd.com")
    val hash = org.apache.spark.sql.catalyst.expressions.XXH64.hashUnsafeBytes(s.getBaseObject, s.getBaseOffset, s.numBytes(), sparkMagicSeedNumber)
    val result = shiftMod(hash, 10000)
    println(result)
    assertResult(expected)(result)
  }

  "shiftMod zero" should "move the value up to allow unk at zero" in {
    val expected = 1
    val index = 0
    val result = shiftMod(index, 5)
    println(result)
    assertResult(expected)(result)
  }

}
