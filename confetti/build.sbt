name := "confetti"

version := "0.1.0"

scalaVersion := "2.12.15"

val awsVersion = "1.12.654"
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"


libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-logs" % awsVersion,
  "log4j" % "log4j" % "1.2.17",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,
  "com.thetradedesk" %% "eldorado-core" % "1.0.285-spark-3.2.1"
)

