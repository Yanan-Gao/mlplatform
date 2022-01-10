# Benchmark Suite


A Scala Spark Library for TTD Benchmarks.
More details can be found under the Benchmark Suite confluence page:
https://atlassian.thetradedesk.com/confluence/display/DA/Benchmark+Suite

## Example Usage on Databricks
```scala
import com.github.nscala_time.time.Imports._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.linalg.Vectors
import com.ttd.benchmarks.automl.RandomSplitter
import com.ttd.benchmarks.demographic.AgeBenchmark
import com.ttd.benchmarks.util.logging.DateRange
 
val lr = new LinearRegression()
  .setMaxIter(10)
val paramGrid: Array[ParamMap] = new ParamGridBuilder()
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .build()
val now = DateTime.now()
val dates = DateRange.range(now, now + 2.hour, 1.hour)
 
val mlflowExperiment = MlFlowExperiment("/Users/christopher.hawkes@thetradedesk.com/Experiments/my-experiment-name")

val benchmarkTest = new AgeBenchmark[LinearRegressionModel](lr, dates, paramGrid)
  .setParallelism(1)
  .setFeatureCol("features")
  .setRegion("JP")
  .setMlFlowExperiment(Some(mlflowExperiment))
  .setTrainOrSplit(Left(RandomSplitter(trainRatio = 0.8, seed = 123L)))
 
val features = spark.createDataFrame(Array(
  (1, Vectors.dense(0.1)),
  (2, Vectors.dense(0.25)),
  (3, Vectors.dense(0.65))
)).toDF("features", "label")
 
val model = benchmarkTest.fit(features)
val predictions = model.transform(features)
```

