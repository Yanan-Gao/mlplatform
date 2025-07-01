import java.time.{Clock, LocalDateTime}
import sbtrelease.Vcs

name := "audience"

version := "0.0.0"

scalaVersion := "2.12.15"

// For now, we can define a different spark version by passing -DsparkVersion
val sparkVersion = sys.props.getOrElse("sparkVersion", "3.2.1")
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

//unmanagedJars in Compile += baseDirectory.value / "geronimo.jar"

val logback = ExclusionRule("ch.qos.logback")
val jacksonCore = ExclusionRule("com.fasterxml.jackson.core")
val guava = ExclusionRule("com.google.guava")
val awsJavaSdkBundle = ExclusionRule("com.amazonaws", "aws-java-sdk-bundle")


val availsVersion = sparkVersion match {
  case v if v.startsWith("3.5") => "3.0.114"
  case v if v.startsWith("3.2") => "3.0.15"
  case _ => "3.0.15"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "com.typesafe" % "config" % "1.3.0",
  "com.thetradedesk" %% "geronimo" % "0.2.32-SNAPSHOT",
  "com.thetradedesk.segment.client" % "spark_3" % "2.0.9" % "provided",
  "com.thetradedesk" %% "availspipeline.spark-common_no_uniform" % availsVersion,
  "com.linkedin.sparktfrecord" %% "spark-tfrecord" % "0.3.4",

  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,

  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.2.0" % Test,

  "mobi.mtld.da"      % "deviceatlas-common"       % "1.2",
  "mobi.mtld.da"      % "deviceatlas-deviceapi"    % "2.1.2",
  "com.deviceatlas"   % "deviceatlas-enterprise-java"    % "3.2",
  "com.adbrain" %% "neocortex-spark-3" % "3.0.1-SNAPSHOT" excludeAll(guava, awsJavaSdkBundle, logback, jacksonCore) withSources()
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.12.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.7",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.7"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "audience.jar"

assemblyMergeStrategy in assembly := {
  //  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
      case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
      case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
      case PathList("META-INF", "services", file) if file.startsWith("io.openlineage.client.transports.TransportBuilder") => MergeStrategy.first
      case PathList("META-INF", "services", _*) if sparkVersion.startsWith("3.5") => MergeStrategy.concat
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
