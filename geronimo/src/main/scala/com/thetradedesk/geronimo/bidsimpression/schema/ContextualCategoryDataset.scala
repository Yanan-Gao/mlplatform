package com.thetradedesk.geronimo.bidsimpression.schema

import java.sql.Timestamp

case class ContextualCategoryRecord(
     ContextualCategoryId: Long,
     UrlHash: String,
     UrlSchema: String,
     UrlHost: String,
     UrlPath: String,
     UrlQuery: String,
     UrlReference: String,
     UrlLength: Int,
     UpdatedDateTimeUtc: Timestamp,
     ExpireDateTimeUtc: Timestamp,
     InApp: Boolean
   )

object ContextualCategoryDataset {
  val contextual = f"s3://ttd-datapipe-data/parquet/cxt_category_writer/v=1/"
}