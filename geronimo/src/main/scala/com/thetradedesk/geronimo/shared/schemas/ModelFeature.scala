package com.thetradedesk.geronimo.shared.schemas

case class ModelFeature(name: String,
                        dtype: String,
                        cardinality: Option[Int] = None,
                        modelVersion: Int
                       )