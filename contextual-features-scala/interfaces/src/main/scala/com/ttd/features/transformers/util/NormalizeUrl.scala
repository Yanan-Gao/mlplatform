package com.ttd.features.transformers.util

import java.nio.charset.StandardCharsets

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

class NormalizeUrl(override val uid: String) extends UnaryTransformer[String, String, NormalizeUrl]
  with DefaultParamsWritable {
  override protected def createTransformFunc: String => String = NormalizeUrl.normalizeUrl
  override protected def outputDataType: DataType = StringType
}

object NormalizeUrl extends DefaultParamsReadable[NormalizeUrl] {
  /**
   * Removes parameters from url, decodes it and lowercases it.
   * @param url The url to clean
   * @return normalized url
   */
  def normalizeUrl(url: String): String = {
    val i = url.lastIndexOf("?")
    val trimmed = if(i > 0) url.substring(0, i) else url
    try {
      java.net.URLDecoder.decode(trimmed.toLowerCase, StandardCharsets.UTF_8.name());
    } catch {
      case _: Throwable => trimmed
    }
  }

  def apply(inputCol: String, outputCol: String): NormalizeUrl = {
    val normalizeUrl = new NormalizeUrl(Identifiable.randomUID("NormalizeUrl"))
    normalizeUrl.set(normalizeUrl.inputCol -> inputCol)
    normalizeUrl.set(normalizeUrl.outputCol -> outputCol)
  }
}


class NormalizeUrlHistory(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], NormalizeUrlHistory]
  with DefaultParamsWritable {
  override protected def createTransformFunc: Seq[String] => Seq[String] = NormalizeUrlHistory.normalizeUrlHistory
  override protected def outputDataType: DataType = ArrayType(StringType, containsNull = false)
}

object NormalizeUrlHistory extends DefaultParamsReadable[NormalizeUrlHistory] {
  def normalizeUrlHistory(urls: Seq[String]): Seq[String] = {
    urls.map(NormalizeUrl.normalizeUrl).distinct
  }

  def apply(inputCol: String, outputCol: String): NormalizeUrlHistory = {
    val normalizeUrl = new NormalizeUrlHistory(Identifiable.randomUID("NormalizeUrl"))
    normalizeUrl.set(normalizeUrl.inputCol -> inputCol)
    normalizeUrl.set(normalizeUrl.outputCol -> outputCol)
  }
}