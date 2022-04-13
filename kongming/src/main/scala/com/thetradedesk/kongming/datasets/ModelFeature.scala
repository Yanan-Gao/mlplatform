package com.thetradedesk.kongming.datasets

case class ModelFeature(name: String,
                        dtype: String,
                        cardinality: Option[Int] = None,
                        modelVersion: Int
                       )