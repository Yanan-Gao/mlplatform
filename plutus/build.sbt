import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

name := "plutus"

version := "0.0.0"

scalaVersion := "2.12.13"

val sparkVersion = "3.2.1"
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

 // "org.scalactic" %% "scalactic" % "3.2.7" ,
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
 // "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"

  "com.thetradedesk" %% "geronimo" % "0.2.3-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.20-spark-3.2.1"
)



assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "plutus.jar"

assemblyMergeStrategy in assembly := {
//  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
