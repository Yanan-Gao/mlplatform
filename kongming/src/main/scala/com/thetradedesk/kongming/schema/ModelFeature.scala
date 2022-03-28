package com.thetradedesk.kongming.schema

case class ModelFeature(name: String,
                        dtype: String,
                        cardinality: Option[Int] = None,
                        modelVersion: Int
                       )