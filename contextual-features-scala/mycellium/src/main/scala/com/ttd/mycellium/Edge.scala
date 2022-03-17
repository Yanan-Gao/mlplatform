package com.ttd.mycellium

import enumeratum.values._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, hash, lit}

import scala.collection.immutable

// IntEnumEntry will make sure, at compile-time, that multiple members do not share the same value
sealed abstract class Edge(val value: Int) extends IntEnumEntry {
  def idColumn: Column =
    hash(col("v_id1"), col("v_id2"), col("ts"), eType)
  def eType: Column = lit(value)
}

object Edge extends IntEnum[Edge] {
  case object UserIDBrowsesSite extends Edge(value = 1)
  case object UserIDTrackedAtLatLong extends Edge(value = 2)
  case object UserIDConnectedFromIPAddress extends Edge(value = 3)
  case object UserIDClicksCreative extends Edge(value = 4)
  case object AdGroupBidsUserID extends Edge(value = 5)
  case object GeoContainsLatLong extends Edge(value = 6)
  case object PublisherHasSite extends Edge(value = 7)
  case object SupplyVendorHasPublisher extends Edge(value = 8)
  case object AdGroupServesUserID extends Edge(value = 9)
  case object CampaignConvertsUserID extends Edge(value = 10)
  case object AdvertiserOwnsCreative extends Edge(value = 11)
  case object AdGroupUsesCreative extends Edge(value = 12)
  case object AdvertiserRunsCampaign extends Edge(value = 13)
  case object CampaignHasAdGroup extends Edge(value = 14)

  val values: immutable.IndexedSeq[Edge] = findValues
}