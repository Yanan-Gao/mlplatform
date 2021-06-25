package com.thetradedesk.plutus.data.schema

final case class ModelFeature(name: String,
                              dtype: String,
                              cardinality: Option[Int] = None,
                              modelVersion: Int
                             )
