package com.ttd.contextual.datasets.generated.contextual.web.classification

/** Acts as an enum to for web features. */
object WebClassificationFIDs {

  sealed abstract class WebClassificationFID(val fid : Int) extends Ordered[WebClassificationFID]{
    override def compare(that: WebClassificationFID): Int = this.fid - that.fid
  }

  /* Pre-computed classifications */
  case object IABContentClassification extends WebClassificationFID(fid = 1)
}