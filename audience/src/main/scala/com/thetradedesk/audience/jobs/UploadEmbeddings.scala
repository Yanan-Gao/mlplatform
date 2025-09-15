package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling.SIBSampler.isDeviceIdSampled1Percent
import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.types.FloatType

class UploadEmbeddings {
  def run(conf: RelevanceModelOfflineScoringPart2Config): Unit = {
    val dateStr = date.format(dateFormatter)
    val emb_bucket_dest = conf.emb_bucket_dest
    val tdid_emb_path = conf.tdid_emb_path
    val nonsensitive_emb_enum = conf.nonsensitive_emb_enum
    val sensitive_emb_enum = conf.sensitive_emb_enum
    val base_hour = conf.base_hour
    val batch_id = conf.batch_id

    val embedding = spark.read.parquet(tdid_emb_path).withColumnRenamed("TDID", "UIID").cache
    val embeddingFiltered = embedding.filter(isDeviceIdSampled1Percent('TDID))
    val totalAdgroupCount = embeddingFiltered.count;

    // each embedding is 64 * 4 bytes = 256bytes, or 1/4 kb.
    // max size of each files for coldstorage process to pick up is 100mb.
    // total adgroups * 256 bytes = how many bytes we need.
    // devide by 100mb = equals how many files we need.
    // UIID format:  00203888-5c86-455a-b2be-7f93e5c304c4.  use 40 bytes with extra padding just in case.
    val totalPartitions = (totalAdgroupCount * (256 + 40) / (100 * 1048576)).toInt

    //(totalAdgroupCount / 350000).toInt


    // non-sensitive results  type=301/date=20250228/hour=16/batch=01
    // push partition_ids 0 to 6 to first date
    embeddingFiltered
      .select('UIID, 'pred_avg.cast("array<float>").as("embedding"))
      .repartition(totalPartitions)
      .write.format("parquet").mode("overwrite")
      .save(emb_bucket_dest + "type=" + nonsensitive_emb_enum + "/date=" + dateStr + "/hour=" + "%02d".format(base_hour) + "/batch=" + "%02d".format(batch_id));

    // sensitive results type=302/date=20250228/hour=16/batch=01
    embeddingFiltered.select('UIID, 'sen_pred_avg.cast("array<float>").as("embedding"))
      .repartition(totalPartitions)
      .write.format("parquet").mode("overwrite")
      .save(emb_bucket_dest + "type=" + sensitive_emb_enum + "/date=" + dateStr + "/hour=" + "%02d".format(base_hour) + "/batch=" + "%02d".format(batch_id));
  }
}

object UploadEmbeddings
  extends AutoConfigResolvingETLJobBase[RelevanceModelOfflineScoringPart2Config](
    groupName = "audience",
    jobName   = "UploadEmbeddings") {

  override val prometheus = None
// destination setup from hpc team.  https://gitlab.adsrvr.org/thetradedesk/adplatform/-/merge_requests/83817
  // and https://thetradedesk.atlassian.net/wiki/x/_OQlAQ

  override def runETLPipeline(): Unit = {
    val conf = getConfig
    new UploadEmbeddings().run(conf)
  }

  /**
   * for backward compatibility, local test usage.
   * */
  override def loadLegacyConfig(): RelevanceModelOfflineScoringPart2Config = {
    RelevanceModelOfflineScoringLegacyConfigLoader.loadLegacyPart2Config()
  }
}