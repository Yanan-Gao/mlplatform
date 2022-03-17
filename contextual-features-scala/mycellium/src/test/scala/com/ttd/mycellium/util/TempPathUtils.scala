package com.ttd.mycellium.util

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

  def withTempDir(f: String => Unit, optTempBase: Option[String] = None): Unit = {
    val path: Path = Files.createTempDirectory("pre")
    println(s"tmp dir at: $path")
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
