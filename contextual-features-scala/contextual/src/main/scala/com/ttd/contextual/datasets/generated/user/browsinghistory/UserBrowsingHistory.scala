package com.ttd.contextual.datasets.generated.user.browsinghistory

import com.ttd.contextual.datasets.S3Paths
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.GeneratedDataSet
import com.ttd.contextual.datasets.S3Paths
import com.ttd.contextual.datasets.core.SourceDatePartitionedS3DataSet
import com.ttd.contextual.util.elDoradoUtilities.datasets.core._

case class UserBrowsingHistory(version: Int = 1) extends DatePartitionedS3DataSet[UserBrowsingHistoryRecord](
  GeneratedDataSet,
  S3Paths.FEATURE_STORE_ROOT,
  S3Paths.versionedPath(S3Paths.USER_BROWSING_HISTORY_PATH, BrowsingHistoryFIDs.UserBrowsingHistoryFID.fid, version),
  "date",
)

case class UserBrowsingHistoryRecord(
                       UserId: String,
                       Urls: Seq[(String, Seq[Long])])

