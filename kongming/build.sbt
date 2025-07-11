import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

name := "kongming"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"
val prometheusVersion = "0.9.0"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "com.typesafe" % "config" % "1.3.0",

  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,

 // "org.scalactic" %% "scalactic" % "3.2.7" ,
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
 // "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"

  "com.thetradedesk" %% "geronimo" % "0.2.42-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.285-spark-3.2.1",
  "com.thetradedesk" %% "feature_store" % "20250218045228-SNAPSHOT",
)

libraryDependencySchemes += "com.thetradedesk" %% "geronimo" % VersionScheme.Always

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "kongming.jar"

assemblyMergeStrategy in assembly := {
//  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", file) if file.startsWith("io.openlineage.client.transports.TransportBuilder") => MergeStrategy.first
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
