package com.ttd.benchmarks.util.logging

import com.github.nscala_time.time.Imports._

/**
 * Date util class for creating ranges
 */
object DateRange {
  /**
   * Example use that gives all the hours between now and tomorrow (inclusive)
   *
   * DateRange.range(
   *   DateTime.now(),
   *   DateTime.tomorrow(),
   *   1.hour
   * )
   */
  def range(start: DateTime, end: DateTime, step: Duration): Seq[DateTime] = {
    if (start <= end) {
      start +: range(start.plus(step), end, step)
    } else {
      Seq()
    }
  }
}
