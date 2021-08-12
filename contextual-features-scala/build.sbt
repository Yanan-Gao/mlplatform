import java.time.{Clock, LocalDateTime}

import sbtrelease.Vcs

val _name = "contextual-features-scala"
val _version = "0.1"
val _scalaVersion = "2.12.12"
val sparkVersion = "3.1.1"
val hadoopAWSVersion = "3.2.1"

name := _name
version := _version
scalaVersion := _scalaVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-graphx" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-aws"  % hadoopAWSVersion % Provided,
  "org.apache.hadoop" % "hadoop-client" % hadoopAWSVersion % Provided,
  "net.ruippeixotog" %% "scala-scraper" % "2.1.0",
)

libraryDependencies += "commons-io" % "commons-io" % "2.7"

libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.1" % Provided

libraryDependencies += "com.swoop" %% "spark-alchemy" % "1.1.0"

libraryDependencies += "org.jsoup" % "jsoup" % "1.13.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.5" % Test

libraryDependencies += "org.mockito" %% "mockito-scala-scalatest" % "1.14.8" % Test

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false) 
//assemblyJarName in assembly := _name+"_"+_version+".jar"
assemblyJarName in assembly := _name+".jar"

fullClasspath in Runtime := (fullClasspath in (Compile, run)).value

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
