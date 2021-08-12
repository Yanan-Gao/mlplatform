package com.ttd.ml.datasets.generated.user.browsinghistory

import com.ttd.ml.datasets.S3Paths
import com.ttd.ml.datasets.core.SourceDatePartitionedS3DataSet
import com.ttd.ml.util.elDoradoUtilities.datasets.core._

case class UserBrowsingHistory(version: Int = 1) extends SourceDatePartitionedS3DataSet[UserBrowsingHistoryRecord](
  GeneratedDataSet,
  S3Paths.FEATURE_STORE_ROOT,
  S3Paths.versionedPath(S3Paths.USER_BROWSING_HISTORY_PATH, BrowsingHistoryFIDs.UserBrowsingHistoryFID.fid, version),
  "source",
"date"
)

case class UserBrowsingHistoryRecord(
                       UserId: String,
                       Urls: Seq[(String, Seq[Long])])

