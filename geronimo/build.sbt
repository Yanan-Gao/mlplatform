import java.time.{Clock, LocalDateTime}

val sparkVersion = "3.1.1"
val prometheusVersion = "0.9.0"

// project configs
scalaVersion := "2.12.13"
name := "geronimo"
organization := "com.thetradedesk"
version := "0.1.5-SNAPSHOT"

publishTo := {
  if (isSnapshot.value)
    Some("TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot")
  else
    Some("TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release")
}

// Credentials for resolving dependencies
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
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.11" % Test,
  "joda-time" % "joda-time" % "2.9.3" % "provided"
)



assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "geronimo.jar"

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
