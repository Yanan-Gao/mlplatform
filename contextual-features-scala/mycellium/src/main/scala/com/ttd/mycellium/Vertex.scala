package com.ttd.mycellium

import enumeratum.values._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, hash, lit, coalesce, when}
import com.ttd.mycellium.spark.TTDSparkContext.spark.implicits._

import scala.collection.immutable

// IntEnumEntry will make sure, at compile-time, that multiple members do not share the same value
sealed abstract class Vertex(val value: Int) extends IntEnumEntry {
  def idColumn: Column
  def vType: Column = lit(value)
}

object Vertex extends IntEnum[Vertex] {
  case object UserID extends Vertex(value = 1) {
    val badTdid: String = "00000000-0000-0000-0000-000000000000"
    override def idColumn: Column =
      hash(coalesce(
            when($"DeviceAdvertisingId".isNotNull && $"DeviceAdvertisingId" =!= badTdid, $"DeviceAdvertisingId")
              .otherwise(null),
            when($"TDID".isNotNull && $"TDID" =!= badTdid, $"TDID")
              .otherwise(null),
            when($"UnifiedId2".isNotNull && $"UnifiedId2" =!= badTdid, $"UnifiedId2")
              .otherwise(null),
//            when($"IdentityLinkId".isNotNull && $"IdentityLinkId" =!= badTdid, $"IdentityLinkId")
//              .otherwise(null)
      ))
    def idColumnForConversionTracker: Column =
      hash(when($"TDID".isNotNull && $"TDID" =!= badTdid, $"TDID")
          .otherwise(null))
  }
  case object IPAddress extends Vertex(value = 2) {
    override def idColumn: Column = hash(col("IPAddress"))
  }
  case object Site extends Vertex(3) {
    override def idColumn: Column = hash(col("Site"))
  }
  case object GeoLocation extends Vertex(4) {
    override def idColumn: Column = hash(col("Zip"), col("country"))
  }
  case object LatLong extends Vertex(5) {
    override def idColumn: Column = hash(col("Latitude"), col("Longitude"))
  }
  case object Publisher extends Vertex(6) {
    override def idColumn: Column = hash(col("SupplyVendorPublisherId"))
  }
  case object SupplyVendor extends Vertex(7) {
    override def idColumn: Column = hash(col("SupplyVendor"))
  }
  case object AdGroup extends Vertex(8) {
    override def idColumn: Column = hash(col("AdGroupId"))
  }
  case object Advertiser extends Vertex(9) {
    override def idColumn: Column = hash(col("AdvertiserId"))
  }
  case object Creative extends Vertex(10) {
    override def idColumn: Column = hash(col("CreativeId"))
  }
  case object Campaign extends Vertex(11) {
    override def idColumn: Column = hash(col("CampaignId"))
  }
  case object TrackingTag extends Vertex(12) {
    override def idColumn: Column = ???
  }


  val values: immutable.IndexedSeq[Vertex] = findValues
}