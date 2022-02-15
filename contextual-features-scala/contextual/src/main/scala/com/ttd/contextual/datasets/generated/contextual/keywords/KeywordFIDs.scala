package com.ttd.contextual.datasets.generated.contextual.keywords

/** Acts as an enum for keyword features. */
object KeywordFIDs {

  sealed abstract class UrlProfileFID(val fid : Int) extends Ordered[UrlProfileFID]{
    override def compare(that: UrlProfileFID): Int = this.hashCode() - that.hashCode()
  }

  case object YakeFID extends UrlProfileFID(fid = 1)
}
