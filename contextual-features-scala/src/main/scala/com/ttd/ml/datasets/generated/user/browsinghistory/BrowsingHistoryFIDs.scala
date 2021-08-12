package com.ttd.ml.datasets.generated.user.browsinghistory

/** Acts as an enum for user browsing history features. */
object BrowsingHistoryFIDs {

  sealed abstract class BrowsingHistoryFID(val fid : Int) extends Ordered[BrowsingHistoryFID]{
    override def compare(that: BrowsingHistoryFID): Int = this.fid - that.fid
  }

  case object UserBrowsingHistoryFID extends BrowsingHistoryFID(fid = 1)
}
