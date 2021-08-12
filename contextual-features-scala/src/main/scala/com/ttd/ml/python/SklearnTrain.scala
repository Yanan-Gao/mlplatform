package com.ttd.ml.python

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.PythonRunnerWrapper.withTempPath
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, PythonRunnerWrapper}

import scala.sys.process.Process

object SklearnTrain {
  val aggFunc: (String => Array[Byte]) = sklearnModel => {
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
           |  def __call__(self, batchX, batchY):# X, y):
           |    X = batchX.to_numpy().reshape(-1, 1)
           |    reg = ${modelName}().fit(X, batchY)
           |    return np.fromiter((b-128 for b in pickle.dumps(reg)), np.byte)
           |
           |f = open('$path', 'wb');
           |t = ArrayType(ByteType());
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

  def sklearnTrainModel(d: DataFrame, modelName: String, setModelIdCol: String, cols: Column*): DataFrame = {
    val pf: Array[Byte] = aggFunc(modelName)
    val pyf = PythonRunnerWrapper.getRunner(
      pf, PythonRunnerWrapper.getPythonVersion
    )

    val testUDF = UserDefinedPythonFunction(
      name = modelName,
      func = pyf,
      dataType = ArrayType(ByteType),
      pythonEvalType = PythonRunnerWrapper.PYTHON_EVAL_TYPE.SQL_GROUPED_AGG_PANDAS_UDF,
      udfDeterministic = true
    )

    d
      .groupBy(setModelIdCol)
      .agg(testUDF(cols:_*) as "model")
  }
}
