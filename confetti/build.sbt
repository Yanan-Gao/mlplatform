name := "confetti"

version := "0.1.0"

scalaVersion := "2.12.15"

val awsVersion = "1.12.654"
val prometheusVersion = "0.9.0"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,
  "com.thetradedesk" %% "eldorado-core" % "1.0.285-spark-3.2.1"
)

