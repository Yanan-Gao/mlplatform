package com.ttd.ml.spark.config

// import TheTradeDesk.BuildInfo
import com.ttd.ml.util.logging.Logger
import com.typesafe.config.ConfigFactory

sealed trait Environment

case object Production extends Environment

case object ProdTesting extends Environment

case object Testing extends Environment

case object Sandbox extends Environment

case object Local extends Environment

case object LocalParquet extends Environment

case object LocalORC extends Environment

case object ProductionRaw extends Environment

object TTDConfig extends Logger {
  // We need to reset this for tests. Otherwise, this should be a val
  var config = RichConfig(ConfigFactory.load())

  var environment: Environment = config.getStringOption("ttd.env") match {
    case Some("prod") | Some("production") => Production
    case Some("prodRaw") => ProductionRaw
    case Some("prodTest") | Some("prodTesting") | Some("productionTest") | Some("productionTesting") => ProdTesting
    case Some("testing") | Some("test") => Testing
    case Some("sandbox") | Some("sb") => Sandbox
    case Some("local") => Local
    case Some("localParquet") => LocalParquet
    case Some("localORC") => LocalORC
    case _ => Testing
  }

  // log.info(s"Using Build branch: ${BuildInfo.buildBranch.getOrElse("none")}, commit: ${BuildInfo.buildSha.getOrElse("none")}, built on ${BuildInfo.buildTime} by ${BuildInfo.buildUser}")
  log.info(s"Config environment set to $environment")

  def user: String = config.getString("ttd.user", default = System.getenv("USER"))

  // val testDataSetVersionString = s"${BuildInfo.buildUser}:${BuildInfo.buildTime}"
}
