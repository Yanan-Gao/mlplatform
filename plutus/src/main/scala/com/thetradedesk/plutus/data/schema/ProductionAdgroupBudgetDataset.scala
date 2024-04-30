package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.paddedDatePart
import com.thetradedesk.spark.datasets.core.S3Roots

import java.time.LocalDateTime

object ProductionAdgroupBudgetDataset {
  val S3PATH = f"${S3Roots.VERTICA_EXPORT_ROOT}/ExportProductionAdGroupBudgetSnapshot/VerticaAws/"
  def S3PATH_GEN = (dateTime: LocalDateTime) => {
    f"date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}"
  }
}

final case class ProductionAdgroupBudgetData(
                                          AdGroupId: String,
                                          IsValuePacing: Option[Boolean],
                                          IsUsingPIDController: Option[Boolean],
                                        )