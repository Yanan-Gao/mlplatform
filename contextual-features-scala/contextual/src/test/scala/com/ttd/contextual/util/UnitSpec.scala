package com.ttd.contextual.util

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers._

/* Base class that uses has pre-mixed the most used testing traits */
abstract class UnitSpec extends AnyFunSuite with should.Matchers with
  OptionValues with Inside with Inspectors
