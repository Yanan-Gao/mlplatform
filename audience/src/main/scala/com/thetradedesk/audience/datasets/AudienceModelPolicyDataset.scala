package com.thetradedesk.audience.datasets
import com.thetradedesk.audience.{audienceVersionDateFormat, policyTableResultCoalesce, ttdEnv}
import com.thetradedesk.audience.datasets.Model.Model

// https://atlassian.thetradedesk.com/confluence/display/EN/RSM+-+Policy+Table
final case class AudienceModelPolicyRecord(TargetingDataId: Long,

                                           // e.g. conversion tracker id, seed id
                                           SourceId: String,

                                           // [[com.thetradedesk.audience.datasets.DataSource]] e.g. conversion, seed, feedback, SIB
                                           Source: Int,

                                           // [[com.thetradedesk.audience.datasets.GoalType]]e.g. CPA=1, Click=2, Relevance=3
                                           GoalType: Int,

                                           // seed size
                                           Size: Int,

                                           // sample weight is used for weighted downsampling
                                           // we use it to control samples for each targeting data
                                           // default value is 1.0
                                           SampleWeight: Double = 1,

                                           // incremental id from 1, used to avoid targeting data id collision after hash
                                           SyntheticId: Int,

                                           // [[com.thetradedesk.audience.datasets.CrossDeviceVendor]]
                                           // graph is enabled by default
                                           CrossDeviceVendorId: Int,

                                           // if we want to retrain the model for this setting
                                           IsActive: Boolean,
                                           // [[com.thetradedesk.audience.datasets.Tag]]
                                           // A tag indicate if we need to include the seed in incremental training
                                           // step based on model performance or other signal from previous date.
                                           // e.g. Underperform, New, Small, etc
                                           Tag: Int,
                                           // Show how many days the sourceId is inactive
                                           // devault value is 0  
                                           ExpiredDays: Int,
                                          )

case class AudienceModelPolicyWritableDataset(model: Model) extends
  LightWritableDataset[AudienceModelPolicyRecord](s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", policyTableResultCoalesce, dateFormat = audienceVersionDateFormat)

case class AudienceModelPolicyReadableDataset(model: Model) extends
  LightReadableDataset[AudienceModelPolicyRecord](s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", dateFormat = audienceVersionDateFormat)

object Model extends Enumeration {
  type Model = Value
  val None, AEM, RSM = Value
}

object DataSource extends Enumeration {
  type DataSource = Value
  val None, Conversion, SIB, Seed, Feedback, Click = Value
}

object GoalType extends Enumeration {
  type GoalType = Value
  val None, CPA, Click, Relevance = Value
}

object CrossDeviceVendor extends Enumeration {
  type CrossDeviceVendor = Value
  val None, Adbrain, Crosswise, Drawbridge, Tapad, TtdCtv, IdentityLink, Xaxis, AdbrainHH, MIPHH, IAV2Person, IAV2Household, Epsilon = Value
}

object Tag extends Enumeration {
  type Tag = Value
  val None, UnderPerform, New, Small, Existing, Retention, Recall = Value
}