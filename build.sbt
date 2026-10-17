import java.net.URI
import java.security.MessageDigest

import scala.sys.process.Process
import scala.sys.process.ProcessLogger

import laika.helium.config.{HeliumIcon, IconLink}

ThisBuild / scalaVersion     := "3.3.8"
ThisBuild / tlBaseVersion    := "0.4"
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

val catsEffectVersion      = "3.7.1"
val catsTaglessVersion     = "0.16.5"
val fs2Version             = "3.14.0"
val fs2KafkaVersion        = "4.1.3"
val koffiVersion           = "3.3.3"
val lz4JavaVersion         = "1.11.4" // kafka-clients asks for 1.10.2, which has a JVM crash advisory fixed in 1.11.1.
val nodeVersion            = "24"     // The Node.js release the JavaScript backend targets, matching .nvmrc.
val librdkafkaVersion      = "2.15.1"
val librdkafkaSha256       = "23c8575c7d1ced07246cb9cf200c11325b72201fd4134a02414ca869fbdd8ed3"
val munitVersion           = "1.3.6"
val munitCatsEffectVersion = "2.2.1"
val munitScalacheckVersion = "1.3.1"
val catsLawsVersion        = "2.13.0"
val disciplineMunitVersion = "2.0.0"
val slf4jVersion           = "2.0.20"

val librdkafkaPrefix  = settingKey[File]("Directory containing the librdkafka include and lib directories")
val prepareLibrdkafka = taskKey[File]("Downloads, verifies, and builds the pinned librdkafka release")

val librdkafkaLibraryBuild = settingKey[File]("Where the JavaScript backend's library is built, along with the librdkafka it links in")
val librdkafkaMuslBuild    = settingKey[File]("Where the musl library is built in an Alpine container")
val npmLibrdkafkaPackages  = settingKey[File]("Where the @xkafka/librdkafka npm packages are assembled")
val npmLibrdkafkaLocal     = settingKey[File]("The umbrella package the tests install, which names this platform's package by path")
val sharedLibrdkafka       = taskKey[File]("Builds the C shim and the pinned librdkafka, with its own OpenSSL, zlib and zstd, as one shared library")
val npmLibrdkafka          = taskKey[File]("Assembles the @xkafka/librdkafka npm package and the package for this platform")
val forgetStaleLibrdkafka  = taskKey[Unit]("Removes the tests' installed library and npm locks when they come from an earlier library identity")
val npmLibrdkafkaMusl      =
  taskKey[File]("Builds the library for musl on this machine's architecture in an Alpine container, checks it loads there, and packages it")

val repositoryRoot = file(".")
val isMacOs        = sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")

/** A file of this repository as the workflow names it, relative to the repository root. */
def repositoryPath(file: File): String =
  IO.relativize(repositoryRoot.getCanonicalFile, file.getCanonicalFile).getOrElse(sys.error(s"$file is outside the repository"))

val setupNode = WorkflowStep.Use(
  UseRef.Public("actions", "setup-node", "v7"),
  name = Some("Setup Node.js"),
  params = Map(
    "node-version-file"     -> ".nvmrc",
    "cache"                 -> "npm",
    "cache-dependency-path" -> "build.sbt"
  )
)
val publishedArtifactCondition = "github.event_name != 'pull_request' && (startsWith(github.ref, 'refs/tags/v') || github.ref == 'refs/heads/main')"

// librdkafka is built with TLS and compression, which need these headers present before the Native build runs.
val installNativeDependencies = WorkflowStep.Run(
  List("sudo apt-get update", "sudo apt-get install --yes libssl-dev zlib1g-dev libzstd-dev"),
  name = Some("Install librdkafka system dependencies"),
  cond = Some("startsWith(matrix.os, 'ubuntu')")
)

// The JavaScript backend's library takes minutes to build, mostly compiling OpenSSL, so the librdkafka build behind it is kept between runs.
// The cache action takes its paths as one string, a path per line.
def cacheLibrdkafkaLibrary = Def.setting {
  val builds = Seq((client.js / librdkafkaLibraryBuild).value, (client.js / librdkafkaMuslBuild).value)
  WorkflowStep.Use(
    UseRef.Public("actions", "cache", "v4"),
    name = Some("Cache the librdkafka library build"),
    params = Map(
      "path" -> builds.map(repositoryPath).mkString("\n"),
      "key"  -> "librdkafka-library-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('scripts/build-librdkafka-library.sh', 'build.sbt') }}"
    )
  )
}

ThisBuild / githubWorkflowBuildPreamble += setupNode.withCond(
  Some("matrix.project == 'rootJS'")
)
ThisBuild / githubWorkflowOSes := Seq("ubuntu-24.04", "macos-latest")

// The matrix is small, so a failure on one entry should not hide the state of the others.
ThisBuild / githubWorkflowBuildMatrixFailFast := Some(false)

// Only the Native build is worth running twice: it is the one whose result depends on the host toolchain.
ThisBuild / githubWorkflowBuildMatrixExclusions ++= Seq(
  MatrixExclude(Map("os" -> "macos-latest", "project" -> "rootJVM")),
  MatrixExclude(Map("os" -> "macos-latest", "project" -> "rootJS"))
)

ThisBuild / githubWorkflowBuildPreamble += installNativeDependencies.withCond(
  Some("matrix.project == 'rootNative' && startsWith(matrix.os, 'ubuntu')")
)
// Homebrew keeps these keg-only, which the build already accounts for when it locates them.
ThisBuild / githubWorkflowBuildPreamble += WorkflowStep.Run(
  List("brew install openssl@3 zstd zlib"),
  name = Some("Install librdkafka system dependencies"),
  cond = Some("matrix.project == 'rootNative' && startsWith(matrix.os, 'macos')")
)
ThisBuild / githubWorkflowBuildPreamble += cacheLibrdkafkaLibrary.value.withCond(Some("matrix.project == 'rootJS'"))

// The jobs below build with the one Scala version the project has, so they need neither a Scala axis in their matrix nor the `++` that selects it.
ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "npm-librdkafka",
  name = "Prebuilt librdkafka libraries",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    cacheLibrdkafkaLibrary.value,
    WorkflowStep.Sbt(
      List("clientJS/testOnly xkafka.LibrdkafkaPlatformSuite"),
      name = Some("Build this platform's library and load it through its npm package")
    ),
    WorkflowStep.Sbt(
      List("clientJS/npmLibrdkafkaMusl"),
      name = Some("Build the musl library in Alpine and check that it loads there"),
      cond = Some("runner.os == 'Linux'")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "upload-artifact", "v5"),
      name = Some("Keep the packages"),
      params = Map("name" -> "npm-librdkafka-${{ matrix.os }}", "path" -> s"${repositoryPath((client.js / npmLibrdkafkaPackages).value)}/librdkafka*")
    )
  ),
  oses = List("ubuntu-24.04", "ubuntu-24.04-arm", "macos-latest", "macos-15-intel"),
  scalas = Nil,
  sbtStepPreamble = Nil,
  javas = List(JavaSpec.temurin("17")),
  timeoutMinutes = Some(45)
)

// Each version is published once, and the Maven artifacts that depend on it wait for it.
ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "npm-publish",
  name = "Publish the prebuilt librdkafka libraries",
  steps = List(
    WorkflowStep.Checkout,
    WorkflowStep.Use(
      UseRef.Public("actions", "setup-node", "v7"),
      name = Some("Setup Node.js"),
      params = Map("node-version-file" -> ".nvmrc", "registry-url" -> "https://registry.npmjs.org")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "download-artifact", "v6"),
      name = Some("Collect the packages"),
      params = Map("pattern" -> "npm-librdkafka-*", "merge-multiple" -> "true", "path" -> "npm-packages")
    ),
    WorkflowStep.Run(
      List("scripts/publish-npm-librdkafka.sh npm-packages"),
      name = Some("Publish the versions npm does not have yet"),
      env = Map("NODE_AUTH_TOKEN" -> "${{ secrets.NPM_TOKEN }}")
    )
  ),
  cond = Some(publishedArtifactCondition),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  needs = List("npm-librdkafka"),
  timeoutMinutes = Some(15)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "integration",
  name = "Broker-backed integration tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    cacheLibrdkafkaLibrary.value,
    WorkflowStep.Run(
      List("scripts/integration-test.sh"),
      name = Some("Test all three backends against Kafka, including TLS and SASL")
    )
  ),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  timeoutMinutes = Some(45)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "native-leaks",
  name = "Native leak check",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    WorkflowStep.Run(List("sudo apt-get install --yes llvm"), name = Some("Install LeakSanitizer symbolizer")),
    WorkflowStep.Run(List("scripts/native-leak-check.sh"), name = Some("Check the Native downstream application for leaks"))
  ),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  timeoutMinutes = Some(45)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "downstream",
  name = "Published artifact smoke tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    // One of the downstream Native builds links against an installed librdkafka, which is the arrangement the
    // README promises needs no xkafka settings of its own.
    WorkflowStep.Run(
      List("sudo apt-get update", "sudo apt-get install --yes librdkafka-dev"),
      name = Some("Install librdkafka"),
      cond = Some("startsWith(matrix.os, 'ubuntu')")
    ),
    WorkflowStep.Run(
      // sbt draws its progress display with terminal escapes on the runner, and one of them reaches the
      // captured output, which GITHUB_ENV rejects. The display is turned off and anything left is stripped.
      List(
        """version="$(sbt --batch --no-colors -Dsbt.supershell=false --error 'print clientJVM/version' |""" +
          """ tr -d '\r' | sed 's/\x1b\[[0-9;]*[A-Za-z]//g' | tail -n 1)"""",
        """echo "XKAFKA_VERSION=$version" >> "$GITHUB_ENV""""
      ),
      name = Some("Select published version")
    ),
    WorkflowStep.Run(
      List("scripts/downstream-test.sh"),
      name = Some("Test published JVM, JavaScript, and Native artifacts")
    )
  ),
  cond = Some(publishedArtifactCondition),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  needs = List("publish"),
  timeoutMinutes = Some(45)
)

// sbt-typelevel builds its dependency submission and site jobs with the plugin's own default runner, which
// stays on ubuntu-22.04. Both earn their keep, so only their runner is corrected.
ThisBuild / githubWorkflowGeneratedCI := {
  (ThisBuild / githubWorkflowGeneratedCI).value.map { job =>
    if (job.id == "publish") job.withNeeds((job.needs ++ List("integration", "native-leaks", "npm-publish")).distinct)
    else if (Set("dependency-submission", "site").contains(job.id)) job.withOses(List("ubuntu-24.04"))
    else job
  }
}

// librdkafka links TLS, SASL SCRAM/OAUTHBEARER, and gzip/zstd decompression against system libraries rather than
// vendoring them, so both its build and the Scala Native link need to find them. Linux distributions install them
// where the toolchain already looks; Homebrew keeps them keg-only, so on macOS their prefixes have to be discovered
// and passed explicitly. Where pkg-config is installed, librdkafka's configure asks it too, and on macOS it adds
// Homebrew's default OpenSSL to the search path, so the prefixes found here are put ahead of it.
def homebrewPrefixes: Seq[File] = {
  def prefix(formula: String): Option[String] =
    scala.util.Try(Process(Seq("brew", "--prefix", formula)).!!(ProcessLogger(_ => (), _ => ())).trim).toOption.filter(_.nonEmpty)
  if (!isMacOs) Nil else Seq("openssl@3", "zstd", "zlib").flatMap(prefix).map(file).filter(_.isDirectory)
}

def runCommand(log: Logger, command: Seq[String], directory: File, environment: (String, String)*): Unit = {
  val exitCode = Process(command, directory, environment: _*).!(ProcessLogger(log.info(_), log.error(_)))
  if (exitCode != 0)
    sys.error(s"command failed (${command.mkString(" ")})")
}

/** Downloads the pinned librdkafka release into `buildRoot` once, and verifies it. */
def fetchLibrdkafka(log: Logger, buildRoot: File): File = {
  val archive  = buildRoot / s"librdkafka-$librdkafkaVersion.tar.gz"
  val download = URI.create(s"https://github.com/confluentinc/librdkafka/archive/refs/tags/v$librdkafkaVersion.tar.gz").toURL

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
  archive
}

// The C shim both librdkafka backends call. Scala Native compiles it from its resources; the JavaScript backend links it into a shared library.
val librdkafkaShim =
  Seq("xkafka_librdkafka.c", "xkafka_librdkafka_async.c").map(repositoryRoot / "modules" / "client" / "native" / "src" / "main" / "resources" /
    "scala-native" / _)

// The library's identity: the librdkafka release and the shim sources it was built from. It versions the npm packages, so each xkafka build depends
// on a library built from its own shim, and the library reports it, so the JavaScript backend refuses one built from anything else.
val librdkafkaShimId = {
  val digest = MessageDigest.getInstance("SHA-256")
  librdkafkaShim.foreach(source => digest.update(IO.readBytes(source)))
  s"$librdkafkaVersion-${digest.digest().iterator.map(byte => f"${byte & 0xff}%02x").mkString.take(12)}"
}

/** The command that builds the shim and the pinned librdkafka in `buildRoot` into one shared library, the way the npm packages carry it. */
def librdkafkaLibraryCommand(buildRoot: File, archive: File, output: File): Seq[String] =
  Seq(
    (repositoryRoot / "scripts" / "build-librdkafka-library.sh").getAbsolutePath,
    archive.getAbsolutePath,
    buildRoot.getAbsolutePath,
    output.getAbsolutePath,
    librdkafkaShimId
  ) ++ librdkafkaShim.map(_.getAbsolutePath)

// Each platform package by its npm name suffix, which reads as the `os`, `cpu` and, on Linux, the `libc` npm installs it for, and how its
// description names it.
val npmPlatforms = Seq(
  "darwin-arm64"     -> "macOS on Apple silicon",
  "darwin-x64"       -> "macOS on Intel",
  "linux-x64-gnu"    -> "Linux x64 with glibc",
  "linux-arm64-gnu"  -> "Linux arm64 with glibc",
  "linux-x64-musl"   -> "Linux x64 with musl",
  "linux-arm64-musl" -> "Linux arm64 with musl"
)

/** The platform this build runs on, as the platform packages name it. */
def hostNpmPlatform: String = {
  val cpu = sys.props.getOrElse("os.arch", "") match {
    case "aarch64" | "arm64" => "arm64"
    case _                   => "x64"
  }
  val musl = !isMacOs && Option(new File("/lib").listFiles).exists(_.exists(_.getName.startsWith("ld-musl-")))
  if (isMacOs) s"darwin-$cpu" else s"linux-$cpu-${if (musl) "musl" else "gnu"}"
}

/** The package.json of an @xkafka/librdkafka package: the fields they all share, then the package's own. */
def npmPackageJson(name: String, description: String, license: String, fields: (String, scalajsbundler.util.JSON)*): String = {
  import scalajsbundler.util.JSON
  val common = Seq(
    "name"        -> JSON.str(name),
    "version"     -> JSON.str(librdkafkaShimId),
    "description" -> JSON.str(description),
    "license"     -> JSON.str(license),
    "repository"  -> JSON.objStr(Seq("type" -> "git", "url" -> "git+https://github.com/PMBMegalom/xkafka.git"))
  )
  JSON.obj(common ++ fields: _*).toJson + "\n"
}

/** Writes the package of one platform, holding its library and the licenses of everything linked into it, and answers its directory. */
def writePlatformPackage(root: File, platform: String, library: File, sources: File): File = {
  import scalajsbundler.util.JSON
  val label     = npmPlatforms.toMap.getOrElse(platform, sys.error(s"no npm package is built for $platform"))
  val os        = platform.split('-')(0)
  val cpu       = platform.split('-')(1)
  val libc      = platform.split('-').lift(2).map(value => if (value == "gnu") "glibc" else value)
  val fileName  = if (os == "darwin") "libxkafka_librdkafka.dylib" else "libxkafka_librdkafka.so"
  val directory = root / s"librdkafka-$platform"
  val deps      = sources / "mklove" / "deps" / "src"
  val notices   = Seq(
    "xkafka shim (MIT)"                   -> (repositoryRoot / "LICENSE"),
    "librdkafka and the code it bundles"  -> (sources / "LICENSES.txt"),
    "OpenSSL (Apache-2.0)"                -> (deps / "libcrypto" / "LICENSE.txt"),
    "Zstandard (BSD-3-Clause or GPL-2.0)" -> (deps / "libzstd" / "LICENSE"),
    "zlib (Zlib)"                         -> (deps / "zlib" / "LICENSE")
  )

  IO.delete(directory)
  IO.copyFile(library, directory / fileName)
  IO.write(directory / "LICENSES.txt", notices.map { case (title, file) => s"==== $title ====\n\n${IO.read(file)}" }.mkString("\n\n"))
  IO.write(
    directory / "package.json",
    npmPackageJson(
      s"@xkafka/librdkafka-$platform",
      s"librdkafka $librdkafkaVersion with OpenSSL, zlib and zstd, built for xkafka on $label",
      "MIT AND BSD-2-Clause AND Apache-2.0 AND BSD-3-Clause AND Zlib",
      Seq("os" -> JSON.arr(JSON.str(os)), "cpu" -> JSON.arr(JSON.str(cpu))) ++
        libc.map(value => "libc" -> JSON.arr(JSON.str(value))) :+
        ("files" -> JSON.arr(JSON.str(fileName), JSON.str("LICENSES.txt"))): _*
    )
  )
  directory
}

/** Writes the umbrella package, which depends on each platform package, at the versions given, and finds the one npm installed. */
def writeUmbrellaPackage(directory: File, clientBase: File, platforms: Seq[(String, String)]): Unit = {
  import scalajsbundler.util.JSON
  IO.delete(directory)
  IO.copyFile(clientBase / "npm" / "librdkafka" / "index.js", directory / "index.js")
  IO.write(
    directory / "package.json",
    npmPackageJson(
      "@xkafka/librdkafka",
      s"librdkafka $librdkafkaVersion for xkafka on Node.js, prebuilt for each supported platform",
      "MIT",
      "main"                 -> JSON.str("index.js"),
      "files"                -> JSON.arr(JSON.str("index.js")),
      "optionalDependencies" -> JSON.objStr(platforms.map { case (platform, version) => s"@xkafka/librdkafka-$platform" -> version })
    )
  )
}

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
    _.enablePlugins(ScalaJSBundlerPlugin, BuildInfoPlugin)
  )
  .jsSettings(
    Compile / npmDependencies ++= Seq("koffi" -> koffiVersion, "@xkafka/librdkafka" -> librdkafkaShimId),
    Compile / additionalNpmConfig += "license" -> scalajsbundler.util.JSON.str("MIT"),
    // koffi's install script only compiles koffi from source where no prebuilt binary matches the platform, and every platform xkafka
    // supports has one, so the script is declined.
    Compile / additionalNpmConfig += "allowScripts" -> scalajsbundler.util.JSON.obj(s"koffi@$koffiVersion" -> scalajsbundler.util.JSON.bool(false)),
    Test / additionalNpmConfig                      := (Compile / additionalNpmConfig).value,
    librdkafkaLibraryBuild                          := target.value / "librdkafka-shared-build",
    librdkafkaMuslBuild                             := target.value / "librdkafka-musl-build",
    npmLibrdkafkaPackages                           := target.value / "npm",
    npmLibrdkafkaLocal                              := npmLibrdkafkaPackages.value / "local" / librdkafkaShimId / "librdkafka",
    // Built the way the published library is: librdkafka's own build compiles OpenSSL, zlib and zstd from source and links them in statically,
    // and the library exports only the shim's functions, so none of them can clash with the OpenSSL that Node itself carries.
    sharedLibrdkafka := {
      val log       = streams.value.log
      val buildRoot = librdkafkaLibraryBuild.value
      val output    = target.value / "xkafka-librdkafka" / (if (isMacOs) "libxkafka_librdkafka.dylib" else "libxkafka_librdkafka.so")
      val built     = buildRoot / ".shim-id"
      if (!output.isFile || !built.isFile || IO.read(built) != librdkafkaShimId) {
        runCommand(log, librdkafkaLibraryCommand(buildRoot, fetchLibrdkafka(log, buildRoot), output), buildRoot)
        IO.write(built, librdkafkaShimId)
      }
      output
    },
    // The umbrella package as npm gets it, whose platform packages are versioned like it, and a copy these tests install, which names this
    // platform's package by path, so the tests find the library the way an application does.
    npmLibrdkafka := {
      val root     = npmLibrdkafkaPackages.value
      val platform = hostNpmPlatform
      val packaged = writePlatformPackage(root, platform, sharedLibrdkafka.value, librdkafkaLibraryBuild.value / "source")
      writeUmbrellaPackage(root / "librdkafka", baseDirectory.value, npmPlatforms.map { case (name, _) => name -> librdkafkaShimId })
      writeUmbrellaPackage(npmLibrdkafkaLocal.value, baseDirectory.value, Seq(platform -> s"file:${packaged.getAbsolutePath}"))
      root
    },
    // The repository is mounted at its own path, so every path the build passes means the same inside the container.
    npmLibrdkafkaMusl := {
      val log       = streams.value.log
      val buildRoot = librdkafkaLibraryBuild.value
      val output    = librdkafkaMuslBuild.value / "libxkafka_librdkafka.so"
      val build     = librdkafkaLibraryCommand(librdkafkaMuslBuild.value, fetchLibrdkafka(log, buildRoot), output).mkString(" ")
      val project   = repositoryRoot.getAbsolutePath
      val check     =
        s"mkdir -p /tmp/koffi && npm install --silent --no-audit --no-fund --prefix /tmp/koffi koffi@$koffiVersion && " +
          s"node ${(repositoryRoot / "scripts" / "check-librdkafka-library.cjs").getAbsolutePath} /tmp/koffi ${output.getAbsolutePath} $librdkafkaShimId"
      runCommand(
        log,
        Seq(
          "docker",
          "run",
          "--rm",
          "--volume",
          s"$project:$project",
          "--workdir",
          project,
          s"node:$nodeVersion-alpine",
          "sh",
          "-c",
          s"apk add --no-cache build-base perl bash curl linux-headers python3 >/dev/null && $build && $check"
        ),
        repositoryRoot
      )
      // The licenses come from the librdkafka sources, which the host's own build unpacks.
      sharedLibrdkafka.value: Unit
      writePlatformPackage(npmLibrdkafkaPackages.value, hostNpmPlatform.replace("-gnu", "-musl"), output, buildRoot / "source")
    },
    forgetStaleLibrdkafka := {
      val workspace = (Test / npmUpdate / crossTarget).value
      val locks = Seq(baseDirectory.value / "package-lock.json", workspace / "package-lock.json", workspace / "node_modules" / ".package-lock.json")
      def installed(lock: File): Option[String] =
        (play.api.libs.json.Json.parse(IO.read(lock)) \ "packages" \ "node_modules/@xkafka/librdkafka" \ "resolved").asOpt[String]
      if (locks.exists(lock => lock.isFile && installed(lock).exists(!_.contains(s"/local/$librdkafkaShimId/"))))
        IO.delete(locks :+ (workspace / "node_modules" / "@xkafka"))
    },
    // The tests ask for the local copy beside the published version, and the resolution settles that conflict in its favour.
    Test / npmDependencies += "@xkafka/librdkafka" -> s"file:${npmLibrdkafkaLocal.value.getAbsolutePath}",
    Test / npmResolutions                          := Map("@xkafka/librdkafka" -> s"file:${npmLibrdkafkaLocal.value.getAbsolutePath}"),
    // npm links a local directory without installing what it depends on, so the local copies are installed as packages instead.
    Test / npmExtraArgs ++= Seq("--install-links"),
    // The published jar lists its npm dependencies for every configuration, and an application's tests install a library's test dependencies too,
    // so the local copy above is taken out of the list the jar carries.
    Compile / packageBin / mappings := {
      import scalajsbundler.NpmDependencies
      val manifest = NpmDependencies.manifestFileName
      val local    = "@xkafka/librdkafka" -> s"file:${npmLibrdkafkaLocal.value.getAbsolutePath}"
      (Compile / packageBin / mappings).value.map {
        case (file, `manifest`) =>
          val listed    = play.api.libs.json.Json.parse(IO.read(file)).as(NpmDependencies.serializer)
          val published = listed.copy(testDependencies = listed.testDependencies.filterNot(_ == local))
          NpmDependencies.writeManifest(published, target.value / "published-npm-dependencies") -> manifest
        case other => other
      }
    },
    // npm keeps an installed local package, through its locks, even after the path it is asked for changes, so one from an earlier library
    // identity is removed along with the locks that name it.
    Test / npmUpdate := (Test / npmUpdate).dependsOn(npmLibrdkafka, forgetStaleLibrdkafka).value,
    // The identity of the library this build was made with, which the loaded library must report.
    buildInfoPackage := "xkafka.internal.librdkafka",
    buildInfoObject  := "LibraryIdentity",
    buildInfoKeys    := Seq[BuildInfoKey]("shim" -> librdkafkaShimId),
    buildInfoOptions ++= Seq(BuildInfoOption.PackagePrivate, BuildInfoOption.ConstantValue)
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
        val source    = buildRoot / "source"

        def run(command: Seq[String], directory: File): Unit = runCommand(log, command, directory)

        IO.delete(source)
        IO.createDirectory(source)
        run(Seq("tar", "-xzf", fetchLibrdkafka(log, buildRoot).getAbsolutePath, "--strip-components=1", "-C", source.getAbsolutePath), buildRoot)
        val prefixes      = homebrewPrefixes
        val includeFlags  = prefixes.map(prefix => s"-I${prefix.getAbsolutePath}/include")
        val libraryFlags  = prefixes.map(prefix => s"-L${prefix.getAbsolutePath}/lib")
        val pkgConfigPath = prefixes.map(prefix => s"${prefix.getAbsolutePath}/lib/pkgconfig") ++ sys.env.get("PKG_CONFIG_PATH")

        // Homebrew's default OpenSSL is 4, which lacks the engine API librdkafka calls, so OpenSSL 3's pkg-config files have to come first.
        runCommand(
          log,
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
          source,
          (if (pkgConfigPath.isEmpty) Nil else Seq("PKG_CONFIG_PATH" -> pkgConfigPath.mkString(":"))): _*
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
      // The static librdkafka archive leaves TLS and compression symbols undefined, so the final link needs
      // the system libraries it was built against. GNU ld resolves archives left to right and drops members
      // nothing has referenced yet, so librdkafka has to precede them; ld64 resolves across all inputs and
      // does not care, which is why this only shows up on Linux.
      //
      // Homebrew also links OpenSSL 4 into its shared library directory, which the link searches first, and OpenSSL 4 lacks the engine API
      // librdkafka calls. On macOS the OpenSSL librdkafka was built against is therefore named by path.
      val prefixes = homebrewPrefixes
      val openSsl  = prefixes.find(_.getName == "openssl@3") match {
        case Some(prefix) => Seq("libssl", "libcrypto").map(name => s"${prefix.getAbsolutePath}/lib/$name.dylib")
        case None         => Seq("-lssl", "-lcrypto")
      }
      val systemLinkFlags = prefixes.map(prefix => s"-L${prefix.getAbsolutePath}/lib") ++ ("-lrdkafka" +: openSsl) ++ Seq("-lz", "-lzstd")
      config
        .withCompileOptions(_ :+ s"-I${prefix.getAbsolutePath}/include")
        .withLinkingOptions(_ ++ (s"-L${(target.value / "librdkafka-static-link").getAbsolutePath}" +: systemLinkFlags))
    }
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion,
      "at.yawk.lz4"    % "lz4-java"  % lz4JavaVersion,
      "org.slf4j"      % "slf4j-nop" % slf4jVersion % Test
    )
  )

lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(client.jvm)
  .settings(
    tlSiteHelium := tlSiteHelium.value.site.topNavigationBar(
      homeLink = IconLink.external("https://github.com/PMBMegalom/xkafka", HeliumIcon.home)
    )
  )

lazy val root = tlCrossRootProject.aggregate(kernel, client).settings(name := "xkafka")
