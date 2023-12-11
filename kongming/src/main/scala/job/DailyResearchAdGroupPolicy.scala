package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import java.time.LocalDate

object DailyResearchAdGroupPolicy extends KongmingBaseJob {

  override def jobName: String = "DailyResearchAdGroupPolicy"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // default dataset for adgroup policy table which will be updated on a monthly basis
    val defaultPolicyDate = LocalDate.of(2022, 3,15)
    val historicalAdGroupPolicy = AdGroupPolicyDataset(JobExperimentName).readDate(defaultPolicyDate)

    val latestAdGroupPolicy = AdGroupPolicyDataset().readLatestPartitionUpTo(date)
    val mappings = AdGroupPolicyMappingDataset().readLatestPartitionUpTo(date)

    val researchMapping = multiLevelJoinWithPolicy[AdGroupPolicyMappingRecord](mappings, historicalAdGroupPolicy, "left_semi").distinct
    val researchPolicy = latestAdGroupPolicy.join(researchMapping.select("ConfigKey", "ConfigValue").distinct, Seq("ConfigKey", "ConfigValue"), "inner")
      .selectAs[AdGroupPolicyRecord]

    val adGroupPolicyRows = AdGroupPolicyDataset(JobExperimentName).writePartition(researchPolicy, date, Some(1))
    val adGroupPolicyMappingRows = AdGroupPolicyMappingDataset(JobExperimentName).writePartition(researchMapping, date, Some(1))

    Array(adGroupPolicyRows)
  }
}
