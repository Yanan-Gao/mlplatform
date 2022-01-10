import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

val _name = "benchmark-suite"
val _version = "0.2"
val _scalaVersion = "2.12.14"
val sparkVersion = "3.2.0"
val hadoopAWSVersion = "3.3.1"
val xgboostVersion = "1.5.1"

name := _name
version := _version
scalaVersion := _scalaVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
)

libraryDependencies += "com.typesafe" % "config" % "1.4.1"

libraryDependencies += "commons-io" % "commons-io" % "2.7"

libraryDependencies += "org.mlflow" % "mlflow-client" % "1.21.0"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.30.0"

libraryDependencies ++= Seq(
  "ml.dmlc" %% "xgboost4j" % xgboostVersion,
  "ml.dmlc" %% "xgboost4j-spark" % xgboostVersion
)

libraryDependencies += "org.jsoup" % "jsoup" % "1.13.1"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test

libraryDependencies += "org.mockito" %% "mockito-scala-scalatest" % "1.14.8" % Test

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
//assembly / assemblyJarName  := _name+"_"+_version+".jar"
assembly / assemblyJarName := _name+".jar"

Runtime / fullClasspath := (fullClasspath in (Compile, run)).value

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
