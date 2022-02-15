import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

val _name = "spark-features"
val _version = "0.1.0"
val _scalaVersion = "2.12.15"
val sparkVersion = "3.2.1"
val hadoopAWSVersion = "3.2.1"

name := _name
version := _version
scalaVersion := _scalaVersion

ThisBuild / organization := "com.ttd"
ThisBuild / version      := _version

lazy val global = project
  .in(file("."))
  .settings(assemblySettings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    interfaces,
    contextual
  )

lazy val interfaces = project
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "feature-interfaces",
    settings,
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % Test
    )
  )

lazy val contextual = project
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "contextual-features",
    settings,
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.swoop" %% "spark-alchemy" % "1.2.0",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.4.0" % Provided
    )
  )
  .dependsOn(
    interfaces
  )

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-aws"  % hadoopAWSVersion % Provided,
  "org.apache.hadoop" % "hadoop-client" % hadoopAWSVersion % Provided,
  "net.ruippeixotog" %% "scala-scraper" % "2.1.0",
  "com.typesafe" % "config" % "1.4.1",
  "commons-io" % "commons-io" % "2.7",
  "com.github.nscala-time" %% "nscala-time" % "2.30.0",

  // Test
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.14.8" % Test
)


lazy val assemblySettings = Seq(
  assemblyPackageScala / assembleArtifact := false,
  assembly / assemblyJarName := name.value + ".jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf"            => MergeStrategy.concat
    case x => MergeStrategy.first
  },
   Runtime / fullClasspath := (fullClasspath in (Compile, run)).value
)

lazy val vcs = Vcs.detect(new File("."))

lazy val buildInfoSettings = Seq(
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

lazy val settings = assemblySettings ++ buildInfoSettings

