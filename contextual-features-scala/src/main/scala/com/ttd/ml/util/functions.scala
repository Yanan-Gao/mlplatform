package com.ttd.ml.util

import com.ttd.ml.features.web.UrlParser.{cleanPath, parseAllPrefixes}
import org.apache.spark.sql.functions.{explode, regexp_replace, udf}
import com.ttd.ml.features.keywords.workflows.YakeExtractionWorkflow.scoreMapper
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * Commonly used functions available for DataFrame operations. This is meant as a centralised place for easy discovery
 * of all our available functions.
 *
 */
object functions {
  /**
   * Returns a sorted ascending sequence of (keyword, scores) given the result keywords and metadata columns
   * from a spark nlp [[YakePipeline]] annotation
   */
  val scoreMapperUDF: UserDefinedFunction = udf(scoreMapper(_,_))

  /**
   * Returns all of the prefixes of path a url after splitting on '/'
   */
  val udfParseAllPrefixes: UserDefinedFunction = udf(parseAllPrefixes(_))

  /**
   * Cleans Path prefixes to improve coverage on join
   */
  val udfCleanPath: UserDefinedFunction = udf(cleanPath(_, delimiter = ", "))

  /**
   * Finds the largest string from a collection of strings
   */
  val maxParent: UserDefinedFunction = udf((parents: Seq[String]) => {
    parents.maxBy(_.length)
  })

  /**
   * Lists all the prefixes of a string seperated by ", "
   */
  val prefixes: UserDefinedFunction = udf((prefix: String) => {
    prefix.split(", ").inits.toList.map(_.mkString(", ")).filter(_.length > 0)
  })

  /**
   * Explodes all of the prefixs of a column
   */
  val explodeUrlPrefixes: Column => Column = col =>
    explode(udfParseAllPrefixes(regexp_replace(col, " ", "-")))

  /**
   * Finds the maximum value for each column given a matrix of values
   */
  val maxColumnUDF: UserDefinedFunction = udf((confidences: Seq[Seq[Double]]) => {
    val vectors = confidences.toArray
    // get max for each index in vectors
    val maxConf = vectors.head.indices.foldLeft(List[Double]()) {
      case (acc, idx) =>
        val maxOfIdx = vectors.map(_ (idx)).max
        acc :+ maxOfIdx
    }
    maxConf
  })

  /**
   * Finds the trigrams of a string of text
   */
  val trigrams: UserDefinedFunction = udf((text: String) => {
    val substrings: Array[String] = text.take(2000).split("[ ,]+")
    val tris: Seq[String] = (
      for (i <- 1 to 3)
        yield substrings.sliding(i).map(p => p.mkString(" "))
      ).flatten.distinct
    tris
  })
}
