import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

name := "plutus"

version := "0.0"

scalaVersion := "2.12.13"

val sparkVersion = "3.0.1"


libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.slf4s" %% "slf4s-api" % "1.7.25"

libraryDependencies += "io.prometheus" % "simpleclient" % "0.9.0"
libraryDependencies += "io.prometheus" % "simpleclient_common" % "0.9.0"
libraryDependencies += "io.prometheus" % "simpleclient_pushgateway" % "0.9.0"
libraryDependencies += "com.linkedin.sparktfrecord" %% "spark-tfrecord" % "0.3.0"


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "plutus.jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  // this is to ensure we add our HLL UDTs while discarding the rest of spark
  //case PathList("org", "apache", "spark", "sql", ps @ _*) if ps.last startsWith "UDT" => MergeStrategy.first
  //case PathList("org", "apache", "spark", _ @ _*) => MergeStrategy.discard
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

lazy val vcs = Vcs.detect(new File("."))
lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion,
      // actions are computed at compile time
      BuildInfoKey.action("buildTime") {
        LocalDateTime.now(Clock.systemUTC())
      },
      // actions are computed at compile time
      BuildInfoKey.action("buildUser") {
        val user = System.getenv("USER")
        val username = System.getenv("USERNAME")
        if (user != null) user
        else if (username != null) username
        else "Unknown"
      },
      BuildInfoKey.action("buildSha") {
        // Find the current version control system and get the current hash of it
        vcs.map(_.currentHash)
      },
      BuildInfoKey.action("buildBranch") {
        // Find the current version control system and get the current hash of it
        vcs.map(_.currentBranch)
      }
    ),
    buildInfoPackage := "TheTradeDesk"
  )

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
