

name := "feature_store"

organization := "com.thetradedesk"

organizationName := "The TradeDesk"

version := "1.0.3"

scalaVersion := "2.12.15"

// For now, we can define a different spark version by passing -DsparkVersion
val sparkVersion = sys.props.getOrElse("sparkVersion", "3.2.1")

val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
//resolvers += "Central Repository" at "https://repo.maven.apache.org/maven2/"


// Cloudsmith credentials and resolvers
val cloudsmithUser = Option(System.getenv("TTD_CLOUDSMITHUSERNAME"))
  .getOrElse(throw new IllegalStateException("TTD_CLOUDSMITHUSERNAME not defined - check README.md on how to set"))
val cloudsmithPassword = Option(System.getenv("TTD_CLOUDSMITHAPITOKEN"))
  .getOrElse(throw new IllegalStateException("TTD_CLOUDSMITHAPITOKEN not defined - check README.md on how to set"))

credentials += Credentials("Private Repository: thetradedesk/libs-dev", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Private Repository: thetradedesk/libs-staging", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Private Repository: thetradedesk/libs-prod", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Cloudsmith API", "maven.pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
resolvers += "TTDCloudsmithDev" at "https://pkgs.adsrvr.org/basic/libs-dev/maven/"
resolvers += "TTDCloudsmithStaging" at "https://pkgs.adsrvr.org/basic/libs-staging/maven/"
resolvers += "TTDCloudsmithProduction" at "https://pkgs.adsrvr.org/basic/libs-prod/maven/"


credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

publishTo := {
  val publishToNexus = "true".equals(System.getenv("PUBLISH_TO_NEXUS"))
  if (isSnapshot.value) {
    if (publishToNexus)
      Some("TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot")
    else
      Some("TTDCloudsmithStagingPush" at "https://maven.pkgs.adsrvr.org/libs-staging")
  } else
    if (publishToNexus)
      Some("TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release")
    else
      Some("TTDCloudsmithProductionPush" at "https://maven.pkgs.adsrvr.org/libs-prod")
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "com.typesafe" % "config" % "1.3.0",
  "com.thetradedesk" %% "geronimo" % "0.2.28-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.285-spark-3.2.1",
  "com.linkedin.sparktfrecord" %% "spark-tfrecord" % "0.3.4",

  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,

  "com.lihaoyi" %% "upickle" % "3.1.4",

  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.2.0" % Test,
  "com.aerospike" % "aerospike-client-jdk8" % "8.1.0",
  "org.yaml" % "snakeyaml" % "2.2",
  "com.amazonaws" % "aws-java-sdk-glue" % "1.12.654",
  "com.tdunning" % "t-digest" % "3.3"
)

assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
assembly / assemblyJarName := "feature_store.jar"

assembly / assemblyMergeStrategy := {
  //  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

