package com.thetradedesk.featurestore.rsm

// from audience folder
object CommonEnums {
  object DataSource extends Enumeration {
    type DataSource = Value
    val None, Conversion, SIB, Seed, Feedback, Click, TTDOwnData = Value
  }

  object CrossDeviceVendor extends Enumeration {
    type CrossDeviceVendor = Value
    val None, Adbrain, Crosswise, Drawbridge, Tapad, TtdCtv, IdentityLink, Xaxis, AdbrainHH, MIPHH, IAV2Person, IAV2Household, Epsilon = Value
  }

  object FeatureKey extends Enumeration {
    type FeatureKey = Value
    val None, AliasedSupplyPublisherIdCity, SiteZip = Value
  }

  object Grain extends Enumeration {
    type Grain = Value
    val Hourly, Daily = Value
    
    def fromString(s: String): Option[Grain] = {
      s.toLowerCase match {
        case "hour" | "hourly" => Some(Hourly)
        case "day" | "daily" => Some(Daily)
        case _ => None
      }
    }
  }

  object DataIntegrity extends Enumeration {
    type DataIntegrity = Value
    val AllExist, AtLeastOne = Value

    def fromString(s: String): DataIntegrity = {
      s.toLowerCase match {
        case "allexists" | "all_exist" => AllExist
        case "atleastone" | "at_least_one" => AtLeastOne
        case _ => AllExist
      }
    }
  }

  object Tag extends Enumeration {
    type Tag = Value
    val None, UnderPerform, New, Small, Existing, Retention, Recall = Value
  }
}
