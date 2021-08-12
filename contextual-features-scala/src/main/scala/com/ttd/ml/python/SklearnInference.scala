package com.ttd.ml.python

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.PythonRunnerWrapper.withTempPath
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, PythonRunnerWrapper}

import scala.sys.process.Process

object SklearnInference {
  val inferenceFunc: (String => Array[Byte]) = sklearnModel => {
    val splitImport = sklearnModel.split('.').toList
    val fromImport = splitImport.take(splitImport.length - 1).mkString(".")
    val modelName = splitImport.last
    var binaryPythonFunc: Array[Byte] = null
    withTempPath { path =>
      val program =
        s"""
           |from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType, ByteType, StructType, StructField;
           |from pyspark.serializers import CloudPickleSerializer, PickleSerializer;
           |import sklearn;
           |from $fromImport import $modelName
           |import pandas as pd;
           |import numpy as np;
           |import pickle;
           |
           |class CountUDF:
           |  @staticmethod
           |  def __call__(key, batch):
           |    model = pickle.loads(bytes((b for b in (batch["model"][0]+128))))
           |    X = batch["x"].to_numpy().reshape(-1, 1)
           |    preds = model.predict(X)
           |    return pd.DataFrame([(key[0], x, p) for x,p in zip(batch["x"],preds)], columns=["modelID", "x", "pred"])
           |
           |f = open('$path', 'wb');
           |t = StructType([
           |  StructField("modelID", IntegerType()),
           |  StructField("x", IntegerType()),
           |  StructField("pred", FloatType())
           |]);
           |f.write(CloudPickleSerializer().dumps((CountUDF(), t)))
           |""".stripMargin
      Process(
        Seq(
          PythonRunnerWrapper.getPythonPath,
          "-c",
          program),
        None).!!
      binaryPythonFunc = Files.readAllBytes(Paths.get(path))
    }
    assert(binaryPythonFunc != null)
    binaryPythonFunc
  }

  def sklearnTrainInferenceModel(d: DataFrame, modelName: String, setModelIdCol: String, cols: Column*): DataFrame = {
    val pf: Array[Byte] = inferenceFunc(modelName)
    val pyf = PythonRunnerWrapper.getRunner(
      pf, PythonRunnerWrapper.getPythonVersion
    )

    val pyUDF = PythonUDF("test2",
      pyf,
      StructType(Array(
        StructField("modelID", IntegerType),
        StructField("x", IntegerType),
        StructField("pred", FloatType)
      )),
      Seq(col("x"), col("model")).map(_.expr),
      evalType = PythonRunnerWrapper.PYTHON_EVAL_TYPE.SQL_GROUPED_MAP_PANDAS_UDF,
      udfDeterministic = true)

    PythonRunnerWrapper
      .flatMapGroupsInPandas(
        d.groupBy(setModelIdCol),
        pyUDF
      )
  }

}
