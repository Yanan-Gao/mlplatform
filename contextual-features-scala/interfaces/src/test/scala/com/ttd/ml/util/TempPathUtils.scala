package com.ttd.ml.util

import java.nio.file.{Files, Path}

import scala.reflect.io.Directory

object TempPathUtils {
  def withTempPath(f: String => Unit, optTempBase: Option[String] = None): Unit = {
    val path: Path = Files.createTempFile("pre", ".py")
    println(s"tmp file path at: $path")
    try {
      f(path.toAbsolutePath.toString)
    } finally {
      if (!path.toFile.isDirectory) {
        Files.delete(path)
      } else {
        Directory(path.toFile).deleteRecursively()
      }
    }
  }
}
