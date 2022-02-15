package com.ttd.features

import org.joda.time.DateTime

trait FeatureConfig {
  // a source map (s3Path -> dates to be read from that path)
  def sources: Map[String, Seq[DateTime]]
}
