package com.thetradedesk.featurestore.utils

object PathUtils {
  def concatPath(path1: String,
                 path2: String): String = {
    (if (path1.startsWith("/")) "/" else "") + trimPath(path1) + "/" + trimPath(path2)
  }

  // remove slash letter with head and tail
  def trimPath(path: String): String = {
    path.stripPrefix("/").stripSuffix("/")
  }
}
