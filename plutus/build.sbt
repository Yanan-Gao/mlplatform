import Protobuf.{TTDProtoConfig, generateProtoSource}
import com.github.os72.protocjar
import sbt.KeyRanks.BPlusSetting

name := "plutus"

version := "0.0.0"

scalaVersion := "2.12.13"

val sparkVersion = "3.5.5"
val prometheusVersion = "0.9.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

credentials += Credentials(
  "Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

 // "org.scalactic" %% "scalactic" % "3.2.7" ,
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
 // "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"

  "com.thetradedesk" %% "geronimo" % "0.2.45-SNAPSHOT",
  "com.thetradedesk" %% "eldorado-core" % "1.0.322-spark-3.2.1"
)

lazy val protoVersion: SettingKey[String] =
  settingKey[String]("Version of protoc and protobuf lib to use to compile proto files").withRank(BPlusSetting)

inConfig(TTDProtoConfig)(
  Seq(
    sourceDirectory := (configuration / sourceDirectory).value / "main/protobuf",
    sourceDirectories := (sourceDirectory.value :: Nil),
    includeFilter := "*.proto",
    javaSource := (configuration / sourceManaged).value / "main/compiled_protobuf",
    protoVersion := "2.5.0",
    protobufRunProtoc := {
      // Workaround for AARH64-based cpu (such as MacBook M1) to be able to run locally.
      System.setProperty("os.arch", "x86_64")
      val out = "--java_out=" + (TTDProtoConfig / javaSource).value.getCanonicalPath
      args =>
        protocjar.Protoc.runProtoc(s"-v:com.google.protobuf:protoc:${protoVersion.value}" +: out +: args.toArray)
    }
  )
)

cleanFiles += (TTDProtoConfig / javaSource).value
Compile / sourceGenerators += generateProtoSource.taskValue
libraryDependencies += ("com.google.protobuf" % "protobuf-java" % (TTDProtoConfig / protoVersion).value)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "plutus.jar"

assemblyMergeStrategy in assembly := {
//  case PathList("org", "apache", "hadoop", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "scala", _@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "sql", "execution", _@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", file) if file.startsWith("io.openlineage.client.transports.TransportBuilder") => MergeStrategy.first
  case PathList("META-INF", "services", file) if file.startsWith("io.opentelemetry.exporter.internal.grpc.GrpcSenderProvider") => MergeStrategy.first
  case PathList("META-INF", _@_*) => MergeStrategy.discard

  case _ => MergeStrategy.first
}

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// note: this is required for circe and spark to work properly
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll,
  ShadeRule.rename("okhttp3.**" -> "shade.okhttp3.@1").inAll,
  ShadeRule.rename("okio.**" -> "shade.okio.@1").inAll)
