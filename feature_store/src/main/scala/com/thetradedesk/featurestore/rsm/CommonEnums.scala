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
}
