package com.thetradedesk.audience.datasets

final case class AudienceModelPolicyRecord(TargetingDataId: Long,

                                           // e.g. conversion tracker id, seed id
                                           SourceId: Long,

                                           // [[com.thetradedesk.audience.datasets.DataSource]] e.g. conversion, seed, feedback, SIB
                                           Source: Int,

                                           // e.g. CPA=1, Click=2, Relevance=3
                                           KpiType: Int,

                                           // seed size
                                           Size: Int,

                                           // sample weight is used for weighted downsampling
                                           // we use it to control samples for each targeting data
                                           // default value is 1.0
                                           SampleWeight: Double = 1,

                                           // incremental id from 1, used to avoid targeting data id collision after hash
                                           OrderId: Int,

                                           // [[com.thetradedesk.audience.datasets.CrossDeviceVendor]]
                                           // graph is enabled by default
                                           CrossDeviceVendorId: Int,

                                           // [[com.thetradedesk.audience.datasets.Model]] which model this setting is for, 1:AEM 2: RSM
                                           Model: Int
                                          )

case class AudienceModelPolicyDataset() extends
  LightReadableDataset[AudienceModelPolicyRecord](s"/xxx/TBC", "TBC")

object Model {
  val None = 0
  val AEM = 1
  val RSM = 2
}

object DataSource {
  val None = 0
  val Conversion = 1
  val SIB = 2
  val Seed = 3
  val Feedback = 4
  val Click = 5
}

object CrossDeviceVendor {
  val None = 0
  val Adbrain = 1
  val Crosswise = 2
  val Drawbridge = 3
  val Tapad = 4
  val TtdCtv = 5
  val IdentityLink = 6
  val Xaxis = 7
  val AdbrainHH = 8
  val MIPHH = 9
  val IAV2Person = 10
  val IAV2Household = 11
  val Epsilon = 12
}