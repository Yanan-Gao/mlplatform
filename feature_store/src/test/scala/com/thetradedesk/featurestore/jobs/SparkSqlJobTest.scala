package com.thetradedesk.featurestore.jobs

import org.scalatest.funsuite.AnyFunSuite

class SparkSqlJobTest extends AnyFunSuite {
  test("resolve variables") {
    val variables = Map(
      "host" -> "example.com",
      "port" -> "8080",
      "ttd.env" -> "test",
      "url" -> "http://${host}:${port}/api",
      "apiEndpoint" -> "${url}/${ttd.env}/v1",
      "database" -> "jdbc:postgresql://${host}:5432/mydb",
      "nested" -> "This references ${apiEndpoint} which uses ${host}"
    )
    val resolved = VariableResolver.resolveVariables(variables)
    assert("http://example.com:8080/api" === resolved("url"))
    assert("http://example.com:8080/api/test/v1" === resolved("apiEndpoint"))
  }

  test("deserialize job json") {
    val resourceStream = getClass.getResourceAsStream("/sql_jobs/spark_sql_01.json")
    val content = scala.io.Source.fromInputStream(resourceStream).mkString
    val job:SparkSqlJobDescription = SparkSqlJob.parseJob(content)
    assert("LUF-offline-aggregation" === job.name)
    assert("__REQUIRED_ARGUMENT__" === job.config("date"))
    assert("__REQUIRED_ARGUMENT__" === job.config("env"))
    assert("__REQUIRED_ARGUMENT__" === job.config("yesterday"))

    assert("s3://thetradedesk-mlplatform-us-east-1/features/feature_store/${env}/online" === job.config("base_s3_path"))
    assert("${base_s3_path}/clicktracker/parquet/date=${date}/hour=23/" === job.input.parquet.get("click_today"))
    assert("${base_s3_path}/user_feature_agg/date=${date}/" === job.output.location)
    assert("csv" === job.output.format)
    assert(1 == job.output.partition)
    assert("select * from   click_today limit 10" === job.sql)
    assert("split" == job.output.partitionBy.get(0))

    val sideOutputs: List[SparkSqlJobSideOutput] = job.output.sideOutputs.get
    assert(1 == sideOutputs.size)
    assert("select abc from __MAIN_DATAFRAME__" === sideOutputs(0).sql)
    assert("${base_s3_path}/user_feature_agg_side/date=${date}/" === sideOutputs(0).location)
    assert("parquet" === sideOutputs(0).format)
    assert(2 == sideOutputs(0).partition)
    assert("dt" === sideOutputs(0).partitionBy.get(0))
  }
}
