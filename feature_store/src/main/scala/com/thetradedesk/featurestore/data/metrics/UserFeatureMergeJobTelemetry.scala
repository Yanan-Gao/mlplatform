package com.thetradedesk.featurestore.data.metrics

import com.thetradedesk.spark.util.prometheus.PrometheusClient

case class UserFeatureMergeJobTelemetry(implicit prometheus: PrometheusClient) {
  val dataLoadingCount = prometheus.createCounter("perf_auto_features_data_read_count", "feature job data reading count", "data_source_name", "tag")

  val averageRecordSize = prometheus.createGauge(s"average_merged_user_feature_size", "average record size of merged user features", "dateTime")
  val recordCount = prometheus.createGauge(s"merge_user_feature_record_count", "record count processed", "dateTime", "result")

  val jobRunningTime = prometheus.createGauge(s"user_feature_merge_job_running_time", "UserFeatureMergeJob running time", "dateTime")
}
