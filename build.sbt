import java.net.URI
import java.security.MessageDigest

import scala.sys.process.Process
import scala.sys.process.ProcessLogger

ThisBuild / scalaVersion    := "3.3.8"
ThisBuild / version         := "0.1.0-SNAPSHOT"
ThisBuild / licenses        := Seq("MIT" -> url("https://opensource.org/license/mit"))
ThisBuild / tlJdkRelease    := Some(17)
ThisBuild / tlFatalWarnings := true

val catsEffectVersion       = "3.7.0"
val catsTaglessVersion      = "0.16.5"
val fs2Version              = "3.13.0"
val fs2KafkaVersion         = "4.0.0"
val confluentKafkaJsVersion = "1.10.1"
val librdkafkaVersion       = "2.15.1"
val librdkafkaSha256        = "23c8575c7d1ced07246cb9cf200c11325b72201fd4134a02414ca869fbdd8ed3"
val munitVersion            = "1.2.0"
val munitCatsEffectVersion  = "2.2.0"
val slf4jVersion            = "1.7.36"

val repositoryRoot    = file(".")
val librdkafkaPrefix  = settingKey[File]("Directory containing the librdkafka include and lib directories")
val prepareLibrdkafka = taskKey[File]("Downloads, verifies, and builds the pinned librdkafka release")

val commonSettings = Seq(
  scalacOptions += "-Wnonunit-statement",
  Compile / packageBin / mappings += (repositoryRoot / "LICENSE" -> "META-INF/LICENSE"),
  libraryDependencies ++= Seq(
    "org.typelevel" %%% "cats-effect"       % catsEffectVersion,
    "org.typelevel" %%% "cats-tagless-core" % catsTaglessVersion,
    "co.fs2"        %%% "fs2-core"          % fs2Version,
    "org.scalameta" %%% "munit"             % munitVersion % Test
  )
)

lazy val kernel = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/kernel"))
  .settings(commonSettings)
  .settings(
    name := "xkafka-kernel"
  )

lazy val client = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .in(file("modules/client"))
  .dependsOn(kernel)
  .settings(commonSettings)
  .settings(
    name                                    := "xkafka-client",
    Compile / packageBin / mappings += (repositoryRoot / "THIRD_PARTY_NOTICES.md" -> "META-INF/THIRD_PARTY_NOTICES.md"),
    libraryDependencies += "org.typelevel" %%% "munit-cats-effect" % munitCatsEffectVersion % Test
  )
  .jsConfigure(
    _.enablePlugins(ScalaJSBundlerPlugin)
  )
  .jsSettings(
    Compile / npmDependencies += "@confluentinc/kafka-javascript" -> confluentKafkaJsVersion,
    Compile / additionalNpmConfig += "license"                    -> scalajsbundler.util.JSON.str("MIT"),
    Compile / additionalNpmConfig += "allowScripts"               -> scalajsbundler.util.JSON.obj(
      s"@confluentinc/kafka-javascript@$confluentKafkaJsVersion" -> scalajsbundler.util.JSON.bool(true)
    ),
    Test / additionalNpmConfig := (Compile / additionalNpmConfig).value
  )
  .nativeSettings(
    librdkafkaPrefix  := sys.env.get("XKAFKA_LIBRDKAFKA_PREFIX").fold(target.value / s"librdkafka-$librdkafkaVersion")(file),
    prepareLibrdkafka := {
      val log         = streams.value.log
      val prefix      = librdkafkaPrefix.value
      val header      = prefix / "include" / "librdkafka" / "rdkafka.h"
      val library     = prefix / "lib" / "librdkafka.a"
      val buildMarker = prefix / ".xkafka-static-build-v2"

      if (sys.env.contains("XKAFKA_LIBRDKAFKA_PREFIX")) {
        if (!header.isFile || !library.isFile)
          sys.error(s"XKAFKA_LIBRDKAFKA_PREFIX does not contain librdkafka: $prefix")
      } else if (!header.isFile || !library.isFile || !buildMarker.isFile) {
        val buildRoot = target.value / "librdkafka-build"
        val archive   = buildRoot / s"librdkafka-$librdkafkaVersion.tar.gz"
        val source    = buildRoot / "source"
        val download  = URI.create(s"https://github.com/confluentinc/librdkafka/archive/refs/tags/v$librdkafkaVersion.tar.gz").toURL

        IO.createDirectory(buildRoot)
        if (!archive.isFile) {
          log.info(s"Downloading librdkafka $librdkafkaVersion")
          val input = download.openStream()
          try IO.write(archive, IO.readBytes(input))
          finally input.close()
        }

        val actualSha256 = MessageDigest.getInstance("SHA-256").digest(IO.readBytes(archive)).iterator.map(byte => f"${byte & 0xff}%02x").mkString
        if (actualSha256 != librdkafkaSha256)
          sys.error(s"librdkafka checksum mismatch: expected $librdkafkaSha256, got $actualSha256")

        def run(command: Seq[String], directory: File): Unit = {
          val exitCode = Process(command, directory).!(ProcessLogger(log.info(_), log.error(_)))
          if (exitCode != 0)
            sys.error(s"command failed (${command.mkString(" ")})")
        }

        IO.delete(source)
        IO.createDirectory(source)
        run(
          Seq(
            "tar",
            "-xzf",
            archive.getAbsolutePath,
            "--strip-components=1",
            "-C",
            source.getAbsolutePath
          ),
          buildRoot
        )
        run(
          Seq(
            "./configure",
            s"--prefix=${prefix.getAbsolutePath}",
            "--enable-static",
            "--disable-ssl",
            "--disable-gssapi",
            "--disable-curl",
            "--disable-zlib",
            "--disable-zstd",
            "--disable-lz4-ext",
            "--CPPFLAGS=-Drwlock_init=xkafka_librdkafka_rwlock_init"
          ),
          source
        )
        run(
          Seq(
            "make",
            "-C",
            "src",
            s"-j${java.lang.Runtime.getRuntime.availableProcessors()}"
          ),
          source
        )
        run(Seq("make", "-C", "src", "install"), source)
        IO.touch(buildMarker)
      }

      val staticLinkDirectory = target.value / "librdkafka-static-link"
      IO.createDirectory(staticLinkDirectory)
      IO.copyFile(library, staticLinkDirectory / "librdkafka.a")

      prefix
    },
    Compile / nativeLink := (Compile / nativeLink).dependsOn(prepareLibrdkafka).value,
    Test / nativeLink    := (Test / nativeLink).dependsOn(prepareLibrdkafka).value,
    nativeConfig         := {
      val config = nativeConfig.value
      val prefix = librdkafkaPrefix.value
      config
        .withCompileOptions(_ :+ s"-I${prefix.getAbsolutePath}/include")
        .withLinkingOptions(_ :+ s"-L${(target.value / "librdkafka-static-link").getAbsolutePath}")
    }
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion,
      "org.slf4j"      % "slf4j-nop" % slf4jVersion % Test
    )
  )

lazy val root = tlCrossRootProject.aggregate(kernel, client).settings(name := "xkafka")
