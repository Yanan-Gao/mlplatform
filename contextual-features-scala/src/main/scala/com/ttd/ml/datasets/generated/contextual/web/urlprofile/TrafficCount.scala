package com.ttd.ml.datasets.generated.contextual.web.urlprofile

import com.ttd.ml.datasets.S3Paths
import com.ttd.ml.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet}

case class AvailsTrafficCount(version: Int = 1) extends DatePartitionedS3DataSet[AvailsTrafficCountRecord](
  GeneratedDataSet,
  S3Paths.FEATURE_STORE_ROOT,
  S3Paths.versionedPath(S3Paths.WEB_URL_PROFILE_PATH, UrlProfileFIDs.AvailsTrafficCount.fid, version))

case class AvailsTrafficCountRecord(
                                   Url: String,
                                   hllTDIDsketch: Array[Byte],
                                   count: Long
                                   )