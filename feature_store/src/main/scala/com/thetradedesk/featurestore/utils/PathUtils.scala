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
}
