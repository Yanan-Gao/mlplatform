package com.ttd.ml.util.elDoradoUtilities.spark

import java.time.{LocalDate, LocalDateTime, LocalTime}

object RichLocalDate {
  implicit class RichLocalDate(val ld: LocalDate) {
    def atEndOfDay(): LocalDateTime = ld.atTime(LocalTime.MAX)
  }

  def getFileDate(date: LocalDate) : Int = {
    date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth
  }
}

