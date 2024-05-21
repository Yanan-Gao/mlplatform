package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import java.time.LocalDate

object DailyResearchAdGroupPolicy extends KongmingBaseJob {

  override def jobName: String = "DailyResearchAdGroupPolicy"

  object Config {
    var Salt = config.getString("DailyResearchAdGroupPolicy.Salt", "salty")
    var SamplingMod = config.getInt("DailyResearchAdGroupPolicy.SamplingMod", 10)
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val (researchPolicy, researchMapping) = generateData(date)

    // default dataset for adgroup policy table which will be updated on a monthly basis
    val adGroupPolicyRows = AdGroupPolicyDataset(JobExperimentName).writePartition(researchPolicy, date, Some(1))
    val adGroupPolicyMappingRows = AdGroupPolicyMappingDataset(JobExperimentName).writePartition(researchMapping, date, Some(1))

    Array(adGroupPolicyRows)
  }

  def generateData(date: LocalDate): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    val fullPolicy = AdGroupPolicyDataset().readLatestPartitionUpTo(date)
    val fullMapping = AdGroupPolicyMappingDataset().readLatestPartitionUpTo(date)

    val researchMapping = fullMapping
      .filter(xxhash64(concat('CampaignId, lit(Config.Salt))) % Config.SamplingMod === lit(0))
      .selectAs[AdGroupPolicyMappingRecord]

    val researchPolicy = fullPolicy
      .join(researchMapping.select('CampaignId, 'ConfigValue).distinct, Seq("ConfigValue"), "left_semi")
      .selectAs[AdGroupPolicyRecord]

    (researchPolicy, researchMapping)
  }
}
