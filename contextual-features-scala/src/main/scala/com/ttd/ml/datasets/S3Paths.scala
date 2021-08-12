package com.ttd.ml.datasets

/**
 * A centralised store of all the relevant s3 locations.
 */
object S3Paths {
  /* Root paths */
  val FEATURE_STORE_ROOT: String = "s3://thetradedesk-mlplatform-us-east-1/feature_store"

  /* Feature Paths */
  val WEB_EMBEDDINGS_PATH: String = "/feature/contextual/web/embeddings"
  val WEB_KEYWORDS_PATH: String = "/feature/contextual/web/keywords"
  val WEB_URL_PROFILE_PATH: String = "/feature/contextual/web/url_profile"
  val USER_BROWSING_HISTORY_PATH: String = "/feature/user/browsing_history"
  val IAB_TAXONOMY: String = "/feature/contextual/web/taxonomy/iab"
  val WEB_CLASSIFICATION_PATH: String = "/feature/contextual/web/classification"

  /* Monitoring Metric Paths */
  val METRICS: String = "/monitoring_metrics/"

  /** Creates the versioned feature path used for storage under an s3 root.
   *
   *  @param path the path under the root for a feature
   *  @param fid     the feature id
   *  @param version the version of this feature
   *  @return A versioned feature path.
   */
  def versionedPath(path: String, fid: Int, version: Int): String = {
    path + s"/fid=${fid}/v=$version"
  }
}

