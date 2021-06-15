package com.thetradedesk.data.schema

final case class ModelFeature(name: String,
                              dtype: String,
                              cardinality: Int,
                              modelVersion: Int
                             )
