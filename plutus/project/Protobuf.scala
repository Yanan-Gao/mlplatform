import sbt.Defaults.collectFiles
import sbt.Keys._
import sbt.{Configuration, Def, IO, _}
import sbtprotobuf.ProtobufPlugin.Keys._

import java.io.File

object Protobuf {

  lazy val TTDProtoConfig: Configuration = Configuration.of("TTDProtoConfig", "TTDProtoConfig")

  lazy val generateProtoSource: Def.Initialize[Task[Seq[File]]] = Def.task {
    println("Generating Java-sources from protobuf")

    val targetDir = (TTDProtoConfig / javaSource).value
    IO.delete(targetDir)
    targetDir.mkdirs()

    val log = streams.value.log
    val schemas = collectFiles(TTDProtoConfig / sourceDirectories, TTDProtoConfig / includeFilter, TTDProtoConfig / excludeFilter)
      .value.toSet[File].map(_.getAbsoluteFile)
    println(schemas.map(_.get).mkString(","))
    val protoc = (TTDProtoConfig / protobufRunProtoc).value

    val osArch = System.getProperty("os.arch")
    // Workaround for AARH64-based cpu (such as MacBook M1) to be able to run locally.
    System.setProperty("os.arch", "x86_64")

    schemas.foreach { file =>
      println(s"Transpiling proto file '$file'")
      val exitCode = protoc(s"-I${file.getParent}" :: file.getCanonicalPath :: Nil)
      if (exitCode != 0)
        sys.error("protoc returned exit code: %d" format exitCode)
    }
    System.setProperty("os.arch", osArch)
    ((TTDProtoConfig / javaSource).value ** "*.java").get.distinct
  }

}

