package org.apache.spark.sql

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.sys.process.Process
import scala.util.Try

object PythonRunnerWrapper {

  private val pythonPath: String = sys.env.getOrElse("PYSPARK_PYTHON", "/usr/bin/python3")
  private val tempBase: String = sys.env.getOrElse("PYSPARK_TMP_PATH", "/opt/spark/work-dir/")
  val PYTHON_EVAL_TYPE: PythonEvalType.type = PythonEvalType

  private lazy val pythonVer: String =
    Process(
      Seq(pythonPath, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
      None).!!.trim()

  def getPythonPath: String = pythonPath
  def getPythonVersion: String = pythonVer

  def withTempPath(f: String => Unit): Unit = {
    val path: Path = Files.createTempFile(Paths.get(tempBase), "pre", ".py")
    try {
      f(path.toAbsolutePath.toString)
    } finally {
      Files.delete(path)
    }
  }

  lazy val isPySparkAvailable: Boolean = Try {
    Process(
      Seq(pythonPath, "-c", "import pyspark"),
      None).!!
    true
  }.getOrElse(false)

  def getRunner(ab: Array[Byte], pythonVer: String): PythonFunction = PythonFunction(
          command = ab,
          envVars = new java.util.HashMap[String, String](),
          pythonIncludes = List.empty[String].asJava,
          pythonExec = "python3",
          pythonVer = pythonVer,
          broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
          accumulator = null
  )

  def mapInPandas(d: DataFrame, func: PythonUDF): DataFrame = {
    d.mapInPandas(func)
  }

  def flatMapGroupsInPandas(d: RelationalGroupedDataset, func: PythonUDF): DataFrame = {
    d.flatMapGroupsInPandas(func)
  }

  def aggInPandas(d: RelationalGroupedDataset, func: Column): DataFrame = {
    d.agg(func)
  }


  def buildPythonFunction(functionName: String, function: String, pandasType: String): PythonFunction = {
    assert(function.contains(functionName), s"functionName: $functionName must be present in function string")
    var binaryPythonFunc: Array[Byte] = null
    withTempPath { path =>
      val standardImports =
        s"""
           |from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType, ByteType, StructType, StructField;
           |from pyspark.serializers import CloudPickleSerializer, PickleSerializer;
           |import sklearn;
           |import pandas as pd;
           |import numpy as np;
           |import pickle;
           |""".stripMargin
      val pickling =
        s"""
           |f = open('$path', 'wb');
           |f.write(CloudPickleSerializer().dumps((${functionName}, ${pandasType})))
           |""".stripMargin
      Process(
        Seq(
          pythonPath,
          "-c",
          standardImports + function + pickling),
        None).!!
      binaryPythonFunc = Files.readAllBytes(Paths.get(path))
    }
    assert(binaryPythonFunc != null)
    PythonRunnerWrapper.getRunner(
      binaryPythonFunc, pythonVer
    )
  }

  def mapInPandasBuilder(functionName: String,
                         function: String,
                         pandasType: String,
                         dataType: DataType,
                         pythonEvalType: Int): UserDefinedPythonFunction = {
    val pyf: PythonFunction = buildPythonFunction(functionName, function, pandasType)

    UserDefinedPythonFunction(
      name = functionName,
      func = pyf,
      dataType = dataType,
      pythonEvalType = pythonEvalType,
      udfDeterministic = true
    )
  }

  def mapInPandasExpression(functionName: String,
                            function: String,
                            pandasType: String,
                            dataType: DataType,
                            pythonEvalType: Int,
                            e: Seq[Expression]): PythonUDF = {
    val pyf: PythonFunction = buildPythonFunction(functionName, function, pandasType)

    PythonUDF(
      name = functionName,
      func = pyf,
      dataType = dataType,
      children = e,
      evalType = pythonEvalType,
      udfDeterministic = true
    )
  }
}
