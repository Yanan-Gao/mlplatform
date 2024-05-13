package com.thetradedesk.featurestore.testutils

import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalatest.Assertions.assertResult
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
object CollectionUtils {
  def compareArrayWithTolerance[T : Numeric](expected: Array[T], actual: Array[T], eps: T): Unit = {
    assertResult(expected.length)(actual.length)
    expected
      .zip(actual)
      .foreach(
        e => e._1 shouldBe (e._2 +- eps)
      )
  }

}
