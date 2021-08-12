package com.ttd.ml.datasets.generated.contextual.web.urlprofile

/** Acts as an enum for url profile features. */
object UrlProfileFIDs {

  sealed abstract class UrlProfileFID(val fid : Int) extends Ordered[UrlProfileFID]{
    override def compare(that: UrlProfileFID): Int = this.hashCode() - that.hashCode()
  }

  case object PageHtmlMetadataFID extends UrlProfileFID(fid = 1)
  case object SparkNLPUrlLanguageFID extends UrlProfileFID(fid = 2)
  case object AvailsTrafficCount extends UrlProfileFID(fid = 3)
}