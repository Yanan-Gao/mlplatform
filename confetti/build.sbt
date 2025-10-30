name := "confetti"

version := "0.1.0"

scalaVersion := "2.12.15"

val awsVersion = "1.12.654"
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

// Cloudsmith credentials and resolvers
val cloudsmithUser = Option(System.getenv("TTD_CLOUDSMITHUSERNAME"))
  .getOrElse(throw new IllegalStateException("TTD_CLOUDSMITHUSERNAME not defined - check README.md on how to set"))
val cloudsmithPassword = Option(System.getenv("TTD_CLOUDSMITHAPITOKEN"))
  .getOrElse(throw new IllegalStateException("TTD_CLOUDSMITHAPITOKEN not defined - check README.md on how to set"))
val jacksonVersion = "2.14.3"

credentials += Credentials("Private Repository: thetradedesk/libs-dev", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Private Repository: thetradedesk/libs-staging", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Private Repository: thetradedesk/libs-prod", "pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
credentials += Credentials("Cloudsmith API", "maven.pkgs.adsrvr.org", cloudsmithUser, cloudsmithPassword)
resolvers += "TTDCloudsmithDev" at "https://pkgs.adsrvr.org/basic/libs-dev/maven/"
resolvers += "TTDCloudsmithStaging" at "https://pkgs.adsrvr.org/basic/libs-staging/maven/"
resolvers += "TTDCloudsmithProduction" at "https://pkgs.adsrvr.org/basic/libs-prod/maven/"

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.2",
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-logs" % awsVersion,
  "com.hubspot.jinjava" % "jinjava" % "2.7.3",
  "com.google.guava" % "guava" % "33.1.0-jre",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_common" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,
  "com.thetradedesk" %% "eldorado-core" % "1.0.285-spark-3.2.1",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.27" % Test,
  "com.typesafe" % "config" % "1.4.2" % Test,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion
)