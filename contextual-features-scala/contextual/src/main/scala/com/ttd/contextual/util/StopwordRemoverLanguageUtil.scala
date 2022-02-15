package com.ttd.contextual.util

import org.apache.spark.ml.feature.StopWordsRemover
import com.johnsnowlabs.nlp.annotators.ld.dl.LanguageDetectorDL

/**
 * Helper class to translate language codes to spark language names
 */
object StopwordRemoverLanguageUtil {
  // iso639-3 language codes
  val sparkMLSuportedLangs = Map(
    "dan" -> "danish",
    "dut" -> "dutch",
    "eng" -> "english",
    "fin" -> "finnish",
    "fra" -> "french",
    "deu" -> "german",
    "hun" -> "hungarian",
    "ita" -> "italian",
    "nor" -> "norwegian",
    "por" -> "portuguese",
    "rus" -> "russian",
    "spa" -> "spanish",
    "swe" -> "swedish",
    "tur" -> "turkish"
  )

  def fromIso3(lang: String): String = {
    sparkMLSuportedLangs.get(lang) match {
      case Some(value) => value
      case None => throw new UnsupportedOperationException(s"language code $lang is not supported")
    }
  }
}
