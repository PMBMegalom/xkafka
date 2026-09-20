import java.net.URI
import java.security.MessageDigest

import scala.sys.process.Process
import scala.sys.process.ProcessLogger

ThisBuild / scalaVersion     := "3.3.8"
ThisBuild / tlBaseVersion    := "0.1"
ThisBuild / organization     := "io.github.pmbmegalom"
ThisBuild / organizationName := "Piermatteo Barambani Megalom"
ThisBuild / homepage         := Some(url("https://github.com/PMBMegalom/xkafka"))
ThisBuild / scmInfo          := Some(
  ScmInfo(
    url("https://github.com/PMBMegalom/xkafka"),
    "scm:git:https://github.com/PMBMegalom/xkafka.git",
    "scm:git:git@github.com:PMBMegalom/xkafka.git"
  )
)
ThisBuild / developers := List(
  tlGitHubDev("PMBMegalom", "Piermatteo Barambani Megalom")
)
ThisBuild / startYear       := Some(2026)
ThisBuild / licenses        := Seq("MIT" -> url("https://opensource.org/license/mit"))
ThisBuild / tlJdkRelease    := Some(17)
ThisBuild / tlFatalWarnings := true

val setupNode = WorkflowStep.Use(
  UseRef.Public("actions", "setup-node", "v7"),
  name = Some("Setup Node.js"),
  params = Map(
    "node-version-file"     -> ".nvmrc",
    "cache"                 -> "npm",
    "cache-dependency-path" -> "modules/client/js/package-lock.json"
  )
)
val publishedArtifactCondition = "github.event_name != 'pull_request' && (startsWith(github.ref, 'refs/tags/v') || github.ref == 'refs/heads/main')"

// librdkafka is built with TLS and compression, which need these headers present before the Native build runs.
val installNativeDependencies = WorkflowStep.Run(
  List("sudo apt-get update", "sudo apt-get install --yes libssl-dev zlib1g-dev libzstd-dev"),
  name = Some("Install librdkafka system dependencies")
)

ThisBuild / githubWorkflowBuildPreamble += setupNode.withCond(
  Some("matrix.project == 'rootJS'")
)
ThisBuild / githubWorkflowBuildPreamble += installNativeDependencies.withCond(
  Some("matrix.project == 'rootNative'")
)
ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "integration",
  name = "Broker-backed integration tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    WorkflowStep.Run(
      List("scripts/integration-test.sh"),
      name = Some("Test all three backends against Kafka")
    )
  ),
  scalas = List("3"),
  javas = List(JavaSpec.temurin("17")),
  timeoutMinutes = Some(45)
)
ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "downstream",
  name = "Published artifact smoke tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    WorkflowStep.Run(
      List("""echo "XKAFKA_VERSION=$(sbt --error 'print clientJVM/version')" >> $GITHUB_ENV"""),
      name = Some("Select published version")
    ),
    WorkflowStep.Run(
      List("scripts/downstream-test.sh"),
      name = Some("Test published JVM, JavaScript, and Native artifacts")
    )
  ),
  cond = Some(publishedArtifactCondition),
  scalas = List("3"),
  javas = List(JavaSpec.temurin("17")),
  needs = List("publish"),
  timeoutMinutes = Some(45)
)

val catsEffectVersion       = "3.7.0"
val catsTaglessVersion      = "0.16.5"
val fs2Version              = "3.13.0"
val fs2KafkaVersion         = "4.0.0"
val confluentKafkaJsVersion = "1.10.1"
val librdkafkaVersion       = "2.15.1"
val librdkafkaSha256        = "23c8575c7d1ced07246cb9cf200c11325b72201fd4134a02414ca869fbdd8ed3"
val munitVersion            = "1.2.0"
val munitCatsEffectVersion  = "2.2.0"
val munitScalacheckVersion  = "1.2.0"
val catsLawsVersion         = "2.13.0"
val disciplineMunitVersion  = "2.0.0"
val slf4jVersion            = "1.7.36"

val repositoryRoot    = file(".")
val librdkafkaPrefix  = settingKey[File]("Directory containing the librdkafka include and lib directories")
val prepareLibrdkafka = taskKey[File]("Downloads, verifies, and builds the pinned librdkafka release")

// librdkafka links TLS, SASL SCRAM/OAUTHBEARER, and gzip/zstd decompression against these system
// libraries rather than vendoring them, so both its build and the Scala Native link need to find them.
// Linux distributions install them where the toolchain already looks; Homebrew keeps them keg-only,
// so on macOS their prefixes have to be discovered and passed explicitly. pkg-config is deliberately
// not used: it is absent from stock macOS, and librdkafka's own configure falls back to compile checks.
val librdkafkaSystemLibraries = Seq("openssl@3", "zstd", "zlib")

val isMacOs = sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")

def commandOutput(command: Seq[String]): Option[String] =
  scala.util.Try(Process(command).!!(ProcessLogger(_ => (), _ => ())).trim).toOption.filter(_.nonEmpty)

def homebrewPrefixes: Seq[File] =
  if (!isMacOs) Nil
  else librdkafkaSystemLibraries.flatMap(formula => commandOutput(Seq("brew", "--prefix", formula)).map(file)).filter(_.isDirectory)

def librdkafkaIncludeFlags: Seq[String] = homebrewPrefixes.map(prefix => s"-I${prefix.getAbsolutePath}/include")

def librdkafkaSearchFlags: Seq[String] = homebrewPrefixes.map(prefix => s"-L${prefix.getAbsolutePath}/lib")

val commonSettings = Seq(
  headerLicense := Some(HeaderLicense.MIT("2026", "xkafka contributors")),
  scalacOptions += "-Wnonunit-statement",
  Compile / packageBin / mappings += (repositoryRoot / "LICENSE" -> "META-INF/LICENSE"),
  libraryDependencies ++= Seq(
    "org.typelevel" %%% "cats-effect"       % catsEffectVersion,
    "org.typelevel" %%% "cats-tagless-core" % catsTaglessVersion,
    "co.fs2"        %%% "fs2-core"          % fs2Version,
    "org.scalameta" %%% "munit"             % munitVersion           % Test,
    "org.typelevel" %%% "munit-cats-effect" % munitCatsEffectVersion % Test,
    "org.scalameta" %%% "munit-scalacheck"  % munitScalacheckVersion % Test,
    "org.typelevel" %%% "cats-laws"         % catsLawsVersion        % Test,
    "org.typelevel" %%% "discipline-munit"  % disciplineMunitVersion % Test
  )
)

lazy val kernel = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/kernel"))
  .settings(commonSettings)
  .settings(
    name        := "xkafka-kernel",
    description := "Portable data types and algebras for xkafka"
  )

lazy val client = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .in(file("modules/client"))
  .dependsOn(kernel)
  .settings(commonSettings)
  .settings(
    name        := "xkafka-client",
    description := "Cross-platform functional Kafka client for Scala",
    Compile / packageBin / mappings += (repositoryRoot / "THIRD_PARTY_NOTICES.md" -> "META-INF/THIRD_PARTY_NOTICES.md")
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
      val buildMarker = prefix / ".xkafka-static-build-v3"

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
        val includeFlags = librdkafkaIncludeFlags
        val libraryFlags = librdkafkaSearchFlags

        run(
          Seq(
            "./configure",
            s"--prefix=${prefix.getAbsolutePath}",
            "--enable-static",
            "--enable-ssl",
            "--enable-zlib",
            "--enable-zstd",
            "--disable-gssapi",
            "--disable-curl",
            "--disable-lz4-ext",
            ("--CPPFLAGS=-Drwlock_init=xkafka_librdkafka_rwlock_init" +: includeFlags).mkString(" ")
          ) ++ (if (libraryFlags.isEmpty) Nil else Seq(s"--LDFLAGS=${libraryFlags.mkString(" ")}")),
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
      // The static librdkafka archive leaves TLS and compression symbols undefined, so the final
      // link needs the system libraries it was built against.
      val systemLinkFlags = librdkafkaSearchFlags ++ Seq("-lssl", "-lcrypto", "-lz", "-lzstd")
      config
        .withCompileOptions(_ :+ s"-I${prefix.getAbsolutePath}/include")
        .withLinkingOptions(_ ++ (s"-L${(target.value / "librdkafka-static-link").getAbsolutePath}" +: systemLinkFlags))
    }
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion,
      "org.slf4j"      % "slf4j-nop" % slf4jVersion % Test
    )
  )

lazy val root = tlCrossRootProject.aggregate(kernel, client).settings(name := "xkafka")
