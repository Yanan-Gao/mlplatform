package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.types.FloatType

object UploadEmbeddings {
// destination setup from hpc team.  https://gitlab.adsrvr.org/thetradedesk/adplatform/-/merge_requests/83817
  // and https://thetradedesk.atlassian.net/wiki/x/_OQlAQ

  val dateStr = date.format(dateFormatter)
  val emb_bucket_dest = config.getString("emb_bucket_dest", "s3://ttd-user-embeddings/dataexport/")
  val tdid_emb_path = config.getString(
    "tdid_emb_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/emb/agg/v=1/date=${dateStr}/")  //from TdidEmbeddingAggregate job.
  val nonsensitive_emb_enum = config.getInt("nonsensitive_emb_enum", 301)
  val sensitive_emb_enum = config.getInt("sensitive_emb_enum", 302)
  val base_hour = config.getInt("base_hour", 0)
  val batch_id = config.getInt("batch_id", 1)


  def main(args: Array[String]): Unit = {

    val embedding = spark.read.parquet(tdid_emb_path).withColumnRenamed("TDID", "UIID").cache
    val totalAdgroupCount = embedding.count;

    // each embedding is 64 * 4 bytes = 256bytes, or 1/4 kb.
    // max size of each files for coldstorage process to pick up is 100mb.
    // total adgroups * 256 bytes = how many bytes we need.
    // devide by 100mb = equals how many files we need.
    // UIID format:  00203888-5c86-455a-b2be-7f93e5c304c4.  use 40 bytes with extra padding just in case.
    val totalPartitions = (totalAdgroupCount * (256 + 40) / (100 * 1048576)).toInt

    //(totalAdgroupCount / 350000).toInt


    // non-sensitive results  type=301/date=20250228/hour=16/batch=01
    // push partition_ids 0 to 6 to first date
    embedding
      .select('UIID, 'non_sen_pred_avg.cast("array<float>").as("embedding"))
      .repartition(totalPartitions)
      .write.format("parquet").mode("overwrite")
      .save(emb_bucket_dest + "type=" + nonsensitive_emb_enum + "/date=" + dateStr + "/hour=" + "%02d".format(base_hour) + "/batch=" + "%02d".format(batch_id));

    // sensitive results type=302/date=20250228/hour=16/batch=01
    embedding.select('UIID, 'sen_pred_avg.cast("array<float>").as("embedding"))
      .repartition(totalPartitions)
      .write.format("parquet").mode("overwrite")
      .save(emb_bucket_dest + "type=" + sensitive_emb_enum + "/date=" + dateStr + "/hour=" + "%02d".format(base_hour) + "/batch=" + "%02d".format(batch_id));
  }
}