import java.time.{Clock, LocalDateTime}

name := "philo"

version := "0.1.0"

scalaVersion := "2.12.13"

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

 // "org.scalactic" %% "scalactic" % "3.2.7" ,
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
 // "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"

  "com.thetradedesk" %% "geronimo" % "0.2.31-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.135-spark-3.2.1"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "philo.jar"

assemblyMergeStrategy in assembly := {
//  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", file) if file.startsWith("io.openlineage.client.transports.TransportBuilder") => MergeStrategy.first
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

// note: this is required for circe and spark to work properly
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
