name := "confetti"

version := "0.1.0"

scalaVersion := "2.12.15"

val awsVersion = "1.12.654"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion
)

