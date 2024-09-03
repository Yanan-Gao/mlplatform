package com.thetradedesk.featurestore.utils

object PathUtils {
  def concatPath(path1: String,
                 path2: String): String = {
    path1.stripSuffix("/") + "/" + trimPath(path2)
  }

  // remove slash letter with head and tail
  def trimPath(path: String): String = {
    path.stripPrefix("/").stripSuffix("/")
  }

  def truncateToParentDirectory(path: String): Option[String] = {
    val separatorIndex = path.lastIndexOf('/')

    if (separatorIndex != -1) {
      Some(path.substring(0, separatorIndex))
    } else {
      None
    }
  }
}
