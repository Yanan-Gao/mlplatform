package com.thetradedesk.plutus.data.schema

import com.thetradedesk.spark.datasets.core.SchemaPolicy.StrictCaseClassSchema
import com.thetradedesk.spark.datasets.core.VerticaBackupsS3DataSet

case class ProductionAdgroupBudgetDataset()
  extends VerticaBackupsS3DataSet[ProductionAdgroupBudgetData](
    "ExportProductionAdGroupBudgetSnapshot/VerticaAws",
    timestampFieldName = "ReportHourUtc",
    schemaPolicy = StrictCaseClassSchema
  ) {}

final case class ProductionAdgroupBudgetData(
                                          AdGroupId: String,
                                          IsValuePacing: Option[Boolean],
                                          IsUsingPIDController: Option[Boolean],
                                        )