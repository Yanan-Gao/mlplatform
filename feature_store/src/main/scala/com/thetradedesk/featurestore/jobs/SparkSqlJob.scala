package com.thetradedesk.featurestore.jobs

import com.thetradedesk.geronimo.shared.readModelFeatures
import com.thetradedesk.spark.util.TTDConfig.config
import io.circe.generic.auto._
import io.circe.parser._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
case class SparkSqlJobInput(parquet: Option[Map[String, String]], csv: Option[Map[String, String]], tfrecord: Option[Map[String, String]])
case class SparkSqlJobSideOutput(sql:String, location: String, format: String, partition:Integer, partitionBy:Option[List[String]])
case class SparkSqlJobOutput(location: String, format: String, partition:Integer, partitionBy:Option[List[String]], sideOutputs: Option[List[SparkSqlJobSideOutput]])


case class SparkSqlJobDescription(name: String, config: Map[String, String], input: SparkSqlJobInput, output: SparkSqlJobOutput, sql:String)

object SparkSqlJob  {
  final val CSV:String = "csv"
  final val PARQUET:String = "parquet"
  final val TFRECORD:String = "tfrecord"
  final val RequiredArgument:String = "__REQUIRED_ARGUMENT__"
  final val MainDataframe:String = "__MAIN_DATAFRAME__"
  final val OneTimeUniqueKey:String = "__DO_NOT_USE_THIS_KEY_IN_JSON__"

  def main(args: Array[String]): Unit = {
    val dryRun = config.getBoolean("dryRun", false)
    val jobConfigPath = config.getString("jobConfigPath", null)
    println(s"dryRun=$dryRun, jobConfigPath=$jobConfigPath")
    if (jobConfigPath == null) {
      throw new RuntimeException("missing parameter:jobConfigPath")
    }
    val jobConfigJsonStr = readModelFeatures(jobConfigPath)
    val job = parseJob(jobConfigJsonStr)
    if (job == null) {
      throw new RuntimeException(s"invalid job config json: ${jobConfigPath}, with content: ${jobConfigJsonStr}")
    }

    // passed arguments first, throw if required argument is missing
    val variables = job.config.map{case (k, v) => {
      var argval = config.getString(k, null)
      if (argval == null) {
        if (v == RequiredArgument) {
          throw new RuntimeException(s"Missing argument ${k}")
        }
        argval = v
      }
      (k,argval)
    }}


    // parquet datasets
    loadInputDatasets(job.input.parquet, PARQUET, variables, dryRun)

    // csv datasets
    loadInputDatasets(job.input.csv, CSV, variables, dryRun)

    // tfrecord datasets
    loadInputDatasets(job.input.tfrecord, TFRECORD, variables, dryRun)


    val output_location = VariableResolver.resolveVariables(variables ++ Map(OneTimeUniqueKey -> job.output.location))(OneTimeUniqueKey)
    println(s"output location $output_location")

    // the SQL itself
    val sql = VariableResolver.resolveVariables(variables ++ Map(OneTimeUniqueKey -> job.sql))(OneTimeUniqueKey)
    println(sql)
    if (! dryRun) {
      val result = spark.sql(sql)
      if (output_location.trim.nonEmpty) {
        println(s"save main result into $output_location")
        writeDataFrame(result, output_location, job.output.format, job.output.partition, job.output.partitionBy)
      } else {
        println(s"skip saving main result as output location is empty: '$output_location'")
      }

      // handle side output
      val sideOutputs = job.output.sideOutputs.getOrElse(List.empty)
      if (sideOutputs.nonEmpty) {
        // register main output as temp view, and cache it
        result.cache()
        result.createOrReplaceTempView(MainDataframe)

        sideOutputs.foreach(so => {
          val so_sql = VariableResolver.resolveVariables(variables ++ Map(OneTimeUniqueKey -> so.sql))(OneTimeUniqueKey)
          println(s"side output sql: $so_sql")
          if (so_sql.trim.nonEmpty) {
            val df = spark.sql(so_sql)
            val so_location = VariableResolver.resolveVariables(variables ++ Map(OneTimeUniqueKey -> so.location))(OneTimeUniqueKey)
            if (so_location.trim.nonEmpty) {
              println(s"save side result to $so_location")
              writeDataFrame(df, so_location, so.format, so.partition, so.partitionBy)
            } else {
              println(s"skip saving side result as output location is empty: '$so_location'")
            }
          } else {
            println("skip side output as the sql is empty")
          }
        })
      }
    }

    spark.catalog.clearCache()
    spark.stop()
  }

  private def loadInputDatasets(inputMap: Option[Map[String, String]], format: String, variables: Map[String, String], dryRun: Boolean): Unit = {
    val resolvedMap  = VariableResolver.resolveVariables(variables ++ inputMap.getOrElse(Map.empty))
    inputMap.getOrElse(Map.empty).foreach{case (k, _) => {
      val resolvedS3Location = resolvedMap(k)
      println(s"load $format $k from $resolvedS3Location")
      if (! dryRun) {
        val reader = spark.read.format(format)
        val df = format match {
          case CSV => reader.option("header", "true").option("inferSchema", "true")
          case _ => reader
        }
        df.load(resolvedS3Location).createOrReplaceTempView(k)
      }
    }}
  }

  def writeDataFrame(df:DataFrame, location:String, format:String, partition:Int, partitionBy:Option[List[String]]) = {
    var writer = df.coalesce(partition).write.format(format).mode("overwrite")

    if ("csv" == format) {
      writer = writer.option("header", "true")  // Use first line as headers
    }
    if ("parquet" == format) {
      writer = writer.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
    }
    partitionBy match {
      case Some(cols) => writer = writer.partitionBy(cols:_*)
      case None =>
    }
    writer.save(location)
  }

  def parseJob(jsonStr: String): SparkSqlJobDescription = {
    decode[SparkSqlJobDescription](jsonStr.replaceAll("\\r\\n|\\r|\\t|\\n", " ")) match {
      case Left(err) => throw new RuntimeException(err)
      case Right(j) => j
    }
  }
}

object VariableResolver {
  // Main resolution function
  def resolveVariables(variables: Map[String, String]): Map[String, String] = {
    val resolved = mutable.Map.empty[String, String]
    val visited = mutable.Set.empty[String]

    def resolve(key: String): String = {
      if (resolved.contains(key)) {
        resolved(key)
      } else if (visited.contains(key)) {
        throw new RuntimeException(s"Circular reference detected for variable '$key'")
      } else {
        visited += key
        variables.get(key) match {
          case Some(value) =>
            val result = resolveReferences(value)
            resolved += (key -> result)
            result
          case None =>
            throw new RuntimeException(s"Undefined variable '$key'")
        }
      }
    }

    def resolveReferences(input: String): String = {
      // This regex matches ${variable} patterns
      val referencePattern = """\$\{([^}]+)\}""".r

      referencePattern.replaceAllIn(input, m => {
        val ref = m.group(1)
        resolve(ref)
      })
    }

    // Resolve all variables
    variables.keys.foreach { key =>
      if (!resolved.contains(key)) {
        resolve(key)
      }
    }

    resolved.toMap
  }
}
