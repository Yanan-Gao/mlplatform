import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

name := "feature_store"

version := "1.0.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
//resolvers += "Central Repository" at "https://repo.maven.apache.org/maven2/"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

publishTo := {
  if (isSnapshot.value)
    Some("TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot")
  else
    Some("TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release")
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "com.typesafe" % "config" % "1.3.0",
  "com.thetradedesk" %% "geronimo" % "0.2.10-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.163-spark-3.2.1",
  "com.linkedin.sparktfrecord" %% "spark-tfrecord" % "0.3.4",

  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,

  "com.lihaoyi" %% "upickle" % "3.1.4",

  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.2.0" % Test
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "feature_store.jar"

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

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

