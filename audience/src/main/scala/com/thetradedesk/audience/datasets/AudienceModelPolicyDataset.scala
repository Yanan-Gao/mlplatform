package com.thetradedesk.audience.datasets
import com.thetradedesk.audience.{audienceVersionDateFormat, policyTableResultCoalesce, ttdEnv, getClassName}
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.spark.util.TTDConfig.config

// https://atlassian.thetradedesk.com/confluence/display/EN/RSM+-+Policy+Table
final case class AudienceModelPolicyRecord(TargetingDataId: Long,

                                           // e.g. conversion tracker id, seed id
                                           SourceId: String,

                                           // [[com.thetradedesk.audience.datasets.DataSource]] e.g. conversion, seed, feedback, SIB
                                           Source: Int,

                                           // [[com.thetradedesk.audience.datasets.GoalType]]e.g. CPA=1, Click=2, Relevance=3
                                           GoalType: Int,

                                           // seed size
                                           Size: Long,

                                           // seed size of active users (seen in past day bidimpression)
                                           ActiveSize: Long,

                                           // seed size of active users with idtype extension (seen in past day bidimpression)
                                           ExtendedActiveSize: Long,

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
                                           // A tag indicates if we need to include the seed in incremental training
                                           // step based on model performance or other signal from previous date.
                                           // e.g. Underperform, New, Small, etc
                                           Tag: Int,
                                           // Show how many days the sourceId is inactive
                                           // devault value is 0  
                                           ExpiredDays: Int,
                                           // Source of the cloud storage  
                                           StorageCloud: Int,
                                          //  top density country
                                           topCountryByDensity: Seq[String],
                                          // [[com.thetradedesk.audience.datasets.PermissionTag]]
                                          // A tag indicates permission with the dataset
                                          PermissionTag: Int,
                                          // incremental id from 1, used to map between source ids and the new Id
                                          MappingId: Int,
                                          AdvertiserId: String,
                                          IndustryCategoryId: BigInt,
                                          IsSensitive: Boolean
                                          )

case class AudienceModelPolicyWritableDataset(model: Model) extends
  LightWritableDataset[AudienceModelPolicyRecord](s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", policyTableResultCoalesce, dateFormat = audienceVersionDateFormat)

case class AudienceModelPolicyWritableDatasetWithExperiment(model: Model, env: String, exp: Option[String]) extends
  LightWritableDataset[AudienceModelPolicyRecord](s"configdata/$env${exp.filter(_.nonEmpty).map(e => s"/$e").getOrElse("")}/audience/policyTable/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", policyTableResultCoalesce, dateFormat = audienceVersionDateFormat)

case class AudienceModelPolicyReadableDataset(model: Model) extends 
  LightReadableDataset[AudienceModelPolicyRecord](s"configdata/${config.getString(s"${getClassName(AudienceModelPolicyReadableDataset)}ReadEnv", ttdEnv)}/audience/policyTable/${model}/v=1", "s3a://thetradedesk-mlplatform-us-east-1/", dateFormat = audienceVersionDateFormat)

object Model extends Enumeration {
  type Model = Value
  val None, AEM, RSM, RSMV2 = Value
}

object DataSource extends Enumeration {
  type DataSource = Value
  val None, Conversion, SIB, Seed, Feedback, Click, TTDOwnData = Value
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

object StorageCloud extends Enumeration {
  type Tag = Value
  val AWS, AZURE = Value
}

object PermissionTag extends Enumeration {
  type PermissionTag = Value
  val None, Shared, Private = Value
}
