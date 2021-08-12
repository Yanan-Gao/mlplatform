package com.ttd.ml.util.elDoradoUtilities.datasets.core

sealed trait FileFormat

case object Parquet extends FileFormat

case object Json extends FileFormat

case class Csv(withHeader: Boolean) extends FileFormat

object Csv{
  val WithHeader: Csv = Csv(true)
  val Headerless: Csv = Csv(false)
}

case class Tsv(withHeader: Boolean) extends FileFormat

object Tsv {
  val WithHeader: Tsv = Tsv(true)
  val Headerless: Tsv = Tsv(false)
}

case object ORC extends FileFormat

