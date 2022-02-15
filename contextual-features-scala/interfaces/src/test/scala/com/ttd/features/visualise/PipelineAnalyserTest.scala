package com.ttd.features.visualise

import com.ttd.features.transformers._
import com.ttd.ml.util.TTDSparkTest
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, sum}

class PipelineAnalyserTest extends TTDSparkTest {
  test("PipelineAnalyser can analyse pipeline") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    df.createOrReplaceGlobalTempView("tempDF")
      val pipe = new Pipeline().setStages(Array(
        IF(condition = true, Filter(col("a") === 1)),
        Filter(col("b") === 1),
        DropNA(),
        Agg(groupBy = Seq(col("a")), aggCols = Seq(sum(col("b")))),
        Join("tempDF", "a", "inner"),
        Select("a"),
        WithColumn("c", col("a") * 1000)
      ))

      println("Analyser:")
      println(new PipelineAnalyser().explore(pipe))
  }

  test("PipelineAnalyser can create graph pipeline") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    df.createOrReplaceGlobalTempView("tempDF")
    val pipe = new Pipeline().setStages(Array(
      IF(condition = true, Filter(col("a") === 1)),
      Filter(col("b") === 1),
      DropNA(),
      Agg(groupBy = Seq(col("a")), aggCols = Seq(sum(col("b")))),
      Join("tempDF", "a", "inner"),
      Select("a"),
      WithColumn("c", col("a") * 1000)
    ))

    println("Analyser:")
    println(new PipelineAnalyser().explore(pipe))
  }

  test("PipelineAnalyser with reading global temp") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    df.createOrReplaceGlobalTempView("tempDF")
    val pipe = new Pipeline().setStages(Array(
      Filter(col("b") === 1),
      DropNA(),
      CreateGlobalTempTable("createdTempTableName"),
      new Pipeline().setStages(Array(
        ReadGlobalTempTable("tempDF"),
        Agg(groupBy = Seq(col("a")), aggCols = Seq(sum(col("b")))),
      )),
      Join("createdTempTableName", "a", "inner"),
      Select("a")
    ))

    println("Analyser:")
    println(new PipelineAnalyser().explore(pipe))
  }

  test("PipelineAnalyser with reading two global temps") {
    val df = spark.createDataFrame(Seq(
      (1, Some(1)), (2, Some(2)), (1, Some(1)), (2, None)
    )).toDF("a", "b")

    df.createOrReplaceGlobalTempView("tempDF")
    val pipe = new Pipeline().setStages(Array(
      Filter(col("b") === 1),
      DropNA(),
      CreateGlobalTempTable("createdTempTableName"),
      new Pipeline().setStages(Array(
        ReadGlobalTempTable("tempDF2"),
        Agg(groupBy = Seq(col("a")), aggCols = Seq(sum(col("b")))),
        CreateGlobalTempTable("aggSum")
      )),
      new Pipeline().setStages(Array(
        ReadGlobalTempTable("tempDF"),
        Agg(groupBy = Seq(col("a")), aggCols = Seq(sum(col("c")))),
      )),
      Join("createdTempTableName", "a", "inner"),
      Join("aggSum", "a", "inner"),
      Select("a")
    ))

    println("Analyser:")
    println(new PipelineAnalyser().explore(pipe))
  }
}
