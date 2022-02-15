package com.ttd.contextual.features.web

import java.net.URI
import java.nio.charset.StandardCharsets

import scala.util.matching.Regex

/** Util functions for manipulating urls. */
object UrlParser {
  /**
   * Given a url string, this function parses it into three of its constituent parts.
   *
   * @param url String.
   * @return Map[(segmentName, segment)] for the domain, path, and query segments.
   */
  def parseUrlToSegments(url: String): Map[String, String] = {
    try {
      val uri = new URI(url)
      Map("domain" -> uri.getHost, "path" -> uri.getPath, "query" -> uri.getQuery)
    } catch  {
      case _: Throwable => Map.empty
    }
  }

  /**
   * Given a url string, this functions returns a list of all its path prefixes.
   *
   * @param url String.
   * @return sequence of all the path prefixes.
   */
  def parseAllPrefixes(url: String): Seq[String] = {
    parsePath(url).split("/").inits.toList.map(_.mkString("/")).filter(_.length > 0)
  }

  /**
   * Given a url string, this functions returns its path.
   *
   * @param url String.
   * @return path component of the url.
   */
  def parsePath(url: String): String = {
//    (http[s]?:\/\/)?([^\/\s]+\/?)(.*?)(\?.*) -> group 3
//    .select('OriginalUrl, regexp_extract('OriginalUrl, "(?<=.*?\\://?)([^\\/\\?]+)", 1).alias("domain"), regexp_extract('OriginalUrl, "(?<=.*?\\://?)([^\\/\\?]+)(.*)", 2).alias("root"))
    try {
      new URI(url).getPath
    } catch  {
      case _: Throwable => ""
    }
  }

  /**
   * Given a url path, returns a cleaned version of the path with digits removed.
   *
   * @param path String.
   * @return cleaned path.
   */
  def cleanPath(path: String, delimiter: String = ", "): String = {
    val replace = (str: String, fromTo: (Regex, String)) => fromTo._1.replaceAllIn(str, fromTo._2)

    val regexes: Seq[(Regex, String)] = Seq(
      ("/[0-9]+(?=/?)".r, ""),
      ("\\.html?".r, ""),   // remove file name ending
      ("^/".r, ""),         // remove starting slash
      ("/$".r, ""),         // remove trailing slash
      ("[-_][-_]*".r, " "), // replace delemiter with space
//      ("[\\d]+".r, ""),     // remove digits
      ("//*".r, "/"),       // remove duplicates
      (" +".r, " "),        // remove duplicates
      ("/".r, delimiter)    // normalises reading for model input
    )

    regexes.foldLeft(path)(replace).toLowerCase()
  }

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

}
