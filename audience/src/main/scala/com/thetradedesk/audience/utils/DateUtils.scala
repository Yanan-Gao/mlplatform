package com.thetradedesk.audience.utils
import java.time.DayOfWeek
import java.time.temporal.ChronoUnit
import com.thetradedesk.audience.datasets.Schedule
import java.time.LocalDate

object DateUtils {   
    def getSchedule(date: LocalDate, startDate: LocalDate, cadence: Int): Schedule.Value = {
        // Calculate the number of days since the reference start to today
        val daysSinceStart = ChronoUnit.DAYS.between(startDate, date)

        // Determine if today is the full training day within the current cadence cycle
        if (daysSinceStart % cadence == 0) {
            Schedule.Full
        } else {
            Schedule.Incremental
        }
    }

}

