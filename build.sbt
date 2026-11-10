import java.net.URI
import java.security.MessageDigest

import scala.sys.process.Process
import scala.sys.process.ProcessLogger

import laika.helium.config.{HeliumIcon, IconLink}
import org.typelevel.sbt.gha.{Permissions, PermissionValue}

ThisBuild / scalaVersion     := "3.3.9"
ThisBuild / tlBaseVersion    := "0.7"
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
val circeVersion           = "0.14.17"
val http4sVersion          = "0.23.38"
val kafkaClientsVersion    = "4.4.0"
val koffiVersion           = "3.3.3"
val lz4JavaVersion         = "1.14.0"    // kafka-clients asks for 1.11.2, which has security advisories fixed in 1.11.4.
val snappyJavaVersion      = "1.1.10.12" // kafka-clients asks for 1.1.10.7, which has security advisories fixed in 1.1.10.9.
val nodeVersion            = "24"        // The Node.js release the JavaScript backend targets, matching .nvmrc.
val librdkafkaVersion      = "2.16.0"
val librdkafkaSha256       = "e6b61de61d3282879a88e4ee3d9f634a8b05bf78a9198dfc25c4820c0c9ed231"
val rocksdbVersion         = "10.1.3"    // rocksdbjni on the JVM, and the libraries its jar carries on the others
val rocksdbSha256          = "e7f925287d9fe2574fb7c7a86f7facaa9b8ab602a2eed0e598bf633528121498"
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

val prepareRocksdb     = taskKey[File]("Downloads and verifies the pinned rocksdbjni jar once, and extracts its RocksDB library for this platform")
val sharedRocksdb      = taskKey[File]("Builds the RocksDB shim into the shared library the @xkafka/rocksdb npm packages carry")
val npmRocksdbLocal    = settingKey[File]("The @xkafka/rocksdb package the tests install, which names this platform's package by path")
val npmRocksdb         = taskKey[File]("Assembles the @xkafka/rocksdb npm package and the package for this platform")
val forgetStaleRocksdb = taskKey[Unit]("Removes the tests' installed RocksDB packages and npm locks when they come from an earlier shim identity")
val npmRocksdbMusl     = taskKey[File]("Builds the RocksDB shim for musl in an Alpine container, checks it opens a store there, and packages it")
val rocksdbShimTest    = taskKey[Unit]("Tests the RocksDB shim in C, through the library the npm package carries, under a leak checker")

val repositoryRoot = file(".")
val isMacOs        = sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")

// The rocksdbjni jar and the libraries taken from it, which the JavaScript and Native builds share.
val rocksdbRoot = repositoryRoot / "target" / s"rocksdb-$rocksdbVersion"

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
// Only what linking the library and packaging it read is kept: the whole build is gigabytes, which pushes the repository's caches out.
// The cache action takes its paths as one string, a path per line.
def cacheLibrdkafkaLibrary = Def.setting {
  val built = Seq(
    ".librdkafka-static-built",
    "source/src/librdkafka-static.a",
    "source/src/rdkafka.h",
    "source/src/rdkafka_mock.h",
    "source/mklove/deps/dest/usr/include"
  )
  val licenses = Seq(
    "source/LICENSES.txt",
    "source/mklove/deps/src/libcrypto/LICENSE.txt",
    "source/mklove/deps/src/libzstd/LICENSE",
    "source/mklove/deps/src/zlib/LICENSE"
  )
  val paths = (built ++ licenses).map((client.js / librdkafkaLibraryBuild).value / _) ++ built.map((client.js / librdkafkaMuslBuild).value / _)
  WorkflowStep.Use(
    UseRef.Public("actions", "cache", "v6"),
    name = Some("Cache the librdkafka library build"),
    params = Map(
      "path" -> paths.map(repositoryPath).mkString("\n"),
      "key"  -> "librdkafka-library-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('scripts/build-librdkafka-library.sh', 'build.sbt') }}"
    )
  )
}

// The rocksdbjni jar is downloaded once and kept, under a key that names the jar by its checksum.
val cacheRocksdb = WorkflowStep.Use(
  UseRef.Public("actions", "cache", "v6"),
  name = Some("Cache the rocksdbjni jar"),
  params = Map("path" -> repositoryPath(rocksdbRoot), "key" -> s"rocksdbjni-${rocksdbSha256.take(16)}")
)

ThisBuild / githubWorkflowBuildPreamble += setupNode.withCond(
  Some("matrix.project == 'rootJS'")
)
// Linking the Native tests takes more than sbt's default heap of 1 GB.
ThisBuild / githubWorkflowEnv += "SBT_OPTS" -> "-Xmx4g"

ThisBuild / githubWorkflowOSes := Seq("ubuntu-24.04", "macos-latest")

// A failure on one entry cancels the others, so nothing waits on a matrix that has already failed.
ThisBuild / githubWorkflowBuildMatrixFailFast := Some(true)

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
ThisBuild / githubWorkflowBuildPreamble += cacheRocksdb.withCond(Some("matrix.project == 'rootJS' || matrix.project == 'rootNative'"))

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
      UseRef.Public("actions", "upload-artifact", "v7"),
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

// npm trusts this workflow to publish the packages, so a job that publishes them proves which workflow it is with a GitHub identity token and
// holds no npm token.
val npmPublishPermissions = Permissions.Specify(
  actions = PermissionValue.None,
  checks = PermissionValue.None,
  contents = PermissionValue.Read,
  deployments = PermissionValue.None,
  idToken = PermissionValue.Write,
  issues = PermissionValue.None,
  packages = PermissionValue.None,
  pages = PermissionValue.None,
  pullRequests = PermissionValue.None,
  repositoryProjects = PermissionValue.None,
  securityEvents = PermissionValue.None,
  statuses = PermissionValue.None
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
      params = Map("node-version-file" -> ".nvmrc")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "download-artifact", "v8"),
      name = Some("Collect the packages"),
      params = Map("pattern" -> "npm-librdkafka-*", "merge-multiple" -> "true", "path" -> "npm-packages")
    ),
    WorkflowStep.Run(
      List("scripts/publish-npm-librdkafka.sh npm-packages"),
      name = Some("Publish the versions npm does not have yet")
    )
  ),
  cond = Some(publishedArtifactCondition),
  permissions = Some(npmPublishPermissions),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  needs = List("npm-librdkafka"),
  timeoutMinutes = Some(15)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "npm-rocksdb",
  name = "Prebuilt RocksDB shims",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    cacheLibrdkafkaLibrary.value,
    cacheRocksdb,
    // The shim test prints the stacks of a harness that hangs, through gdb on Linux.
    WorkflowStep.Run(List("sudo apt-get install --yes gdb"), name = Some("Install gdb"), cond = Some("runner.os == 'Linux'")),
    WorkflowStep.Sbt(List("clientJS/rocksdbShimTest"), name = Some("Test the shim in C under a leak checker")),
    WorkflowStep.Sbt(
      List("clientJS/testOnly xkafka.internal.rocksdb.*"),
      name = Some("Build this platform's package and open a store through it")
    ),
    WorkflowStep.Sbt(
      List("clientJS/npmRocksdbMusl"),
      name = Some("Build the musl shim in Alpine and check that it opens a store there"),
      cond = Some("runner.os == 'Linux'")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "upload-artifact", "v7"),
      name = Some("Keep the packages"),
      params = Map("name" -> "npm-rocksdb-${{ matrix.os }}", "path" -> s"${repositoryPath((client.js / npmLibrdkafkaPackages).value)}/rocksdb*")
    )
  ),
  oses = List("ubuntu-24.04", "ubuntu-24.04-arm", "macos-latest", "macos-15-intel"),
  scalas = Nil,
  sbtStepPreamble = Nil,
  javas = List(JavaSpec.temurin("17")),
  // Each platform's shim is checked on its own, so one that fails leaves the others to report.
  matrixFailFast = Some(false),
  timeoutMinutes = Some(45)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "npm-publish-rocksdb",
  name = "Publish the prebuilt RocksDB shims",
  steps = List(
    WorkflowStep.Checkout,
    WorkflowStep.Use(
      UseRef.Public("actions", "setup-node", "v7"),
      name = Some("Setup Node.js"),
      params = Map("node-version-file" -> ".nvmrc")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "download-artifact", "v8"),
      name = Some("Collect the packages"),
      params = Map("pattern" -> "npm-rocksdb-*", "merge-multiple" -> "true", "path" -> "npm-packages")
    ),
    WorkflowStep.Run(
      List("scripts/publish-npm-rocksdb.sh npm-packages"),
      name = Some("Publish the versions npm does not have yet")
    )
  ),
  cond = Some(publishedArtifactCondition),
  permissions = Some(npmPublishPermissions),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  needs = List("npm-rocksdb"),
  timeoutMinutes = Some(15)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "integration",
  name = "Broker-backed integration tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    cacheLibrdkafkaLibrary.value.withCond(Some("matrix.platform == 'js'")),
    cacheRocksdb.withCond(Some("matrix.platform != 'jvm'")),
    WorkflowStep.Run(
      List("scripts/integration-test.sh ${{ matrix.platform }}"),
      name = Some("Test the backend against Kafka, including TLS and SASL")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "upload-artifact", "v7"),
      name = Some("Keep the broker logs"),
      cond = Some("failure()"),
      params = Map("name" -> "broker-logs-${{ matrix.platform }}", "path" -> "target/broker-logs")
    )
  ),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  // Each backend runs on a runner of its own, with its own brokers, so the three run side by side.
  matrixAdds = Map("platform" -> List("jvm", "js", "native")),
  matrixFailFast = Some(true),
  timeoutMinutes = Some(45)
)

ThisBuild / githubWorkflowAddedJobs += WorkflowJob(
  id = "streams-integration",
  name = "Streams broker tests",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    setupNode,
    installNativeDependencies,
    cacheLibrdkafkaLibrary.value.withCond(Some("matrix.platform == 'js'")),
    cacheRocksdb.withCond(Some("matrix.platform != 'jvm'")),
    WorkflowStep.Run(
      List("scripts/integration-test.sh --streams ${{ matrix.platform }}"),
      name = Some("Test the streams engine against each supported Kafka release, beside Java Streams")
    ),
    WorkflowStep.Use(
      UseRef.Public("actions", "upload-artifact", "v7"),
      name = Some("Keep the broker logs"),
      cond = Some("failure()"),
      params = Map("name" -> "streams-broker-logs-${{ matrix.platform }}", "path" -> "target/broker-logs")
    )
  ),
  oses = List("ubuntu-24.04"),
  scalas = Nil,
  javas = List(JavaSpec.temurin("17")),
  matrixAdds = Map("platform" -> List("jvm", "js", "native")),
  matrixFailFast = Some(true),
  // The JavaScript tests take about 50 minutes, and building the librdkafka library when the cache has none adds over 15.
  timeoutMinutes = Some(75)
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
  id = "js-leaks",
  name = "Scala.js leak check",
  steps = githubWorkflowJobSetup.value.toList ++ List(
    cacheLibrdkafkaLibrary.value,
    WorkflowStep.Run(List("scripts/js-leak-check.sh"), name = Some("Check the Scala.js library's asynchronous calls for leaks"))
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
    // README promises needs no xkafka settings of its own. Ubuntu's own package is older than the release xkafka
    // needs, so it comes from Confluent's repository, as a user's would, at the release xkafka is built with.
    WorkflowStep.Run(
      List(
        "sudo mkdir -p /etc/apt/keyrings",
        "curl -fsSL https://packages.confluent.io/clients/deb/archive.key | sudo gpg --dearmor -o /etc/apt/keyrings/confluent-clients.gpg",
        "echo \"deb [signed-by=/etc/apt/keyrings/confluent-clients.gpg] https://packages.confluent.io/clients/deb $(lsb_release -cs) main\" | sudo tee /etc/apt/sources.list.d/confluent-clients.list",
        "sudo apt-get update",
        s"sudo apt-get install --yes librdkafka-dev=$librdkafkaVersion-1.cflt~ubu24 librdkafka1=$librdkafkaVersion-1.cflt~ubu24"
      ),
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

// Artifacts only carry builds from one job to a later one in the same run, so a week leaves time to re-run a failed job.
def expiring(step: WorkflowStep): WorkflowStep = step match {
  case use: WorkflowStep.Use if uploadsArtifact(use.ref) => use.updatedParams("retention-days", "7")
  case other                                             => other
}
def uploadsArtifact(ref: UseRef): Boolean = ref match {
  case UseRef.Public("actions", "upload-artifact", _) => true
  case _                                              => false
}

// sbt-typelevel builds its dependency submission and site jobs with the plugin's own default runner, which
// stays on ubuntu-22.04. Both earn their keep, so only their runner is corrected.
val publishWaitsFor = List("integration", "streams-integration", "native-leaks", "js-leaks", "npm-publish", "npm-publish-rocksdb")
ThisBuild / githubWorkflowGeneratedCI := {
  (ThisBuild / githubWorkflowGeneratedCI).value.map { job =>
    if (job.id == "publish")
      job.withNeeds((job.needs ++ publishWaitsFor).distinct)
    else if (Set("dependency-submission", "site").contains(job.id)) job.withOses(List("ubuntu-24.04"))
    else job
  }.map(job => job.withSteps(job.steps.map(expiring)))
}

// The Clean workflow deletes every artifact in the repository on each push, those of runs still going included, which
// made the publish job of a run fail when another push came while it ran. Uploads expire instead.
ThisBuild / githubWorkflowIncludeClean := false

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
  Seq("xkafka_librdkafka.c", "xkafka_librdkafka_async.c", "xkafka_crypto.c", "xkafka_socket.c").map(repositoryRoot / "modules" / "client" / "native" /
    "src" / "main" /
    "resources" /
    "scala-native" / _)

// The library's identity: the librdkafka release and everything the npm packages are made from, the shim sources, the script that builds them into
// the library and the umbrella package's loader. It versions the npm packages, so a change to any of them publishes new ones and each xkafka build
// depends on the packages made from its own sources, and the library reports it, so the JavaScript backend refuses one built from anything else.
val librdkafkaShimId = {
  val digest = MessageDigest.getInstance("SHA-256")
  val inputs = librdkafkaShim ++ Seq(
    repositoryRoot / "scripts" / "build-librdkafka-library.sh",
    repositoryRoot / "modules" / "client" / "js" / "npm" / "librdkafka" / "index.js"
  )
  inputs.foreach(source => digest.update(IO.readBytes(source)))
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

/** The package.json of an @xkafka package: the fields they all share, then the package's own. */
def npmPackageJson(name: String, version: String, description: String, license: String, fields: (String, scalajsbundler.util.JSON)*): String = {
  import scalajsbundler.util.JSON
  val common = Seq(
    "name"        -> JSON.str(name),
    "version"     -> JSON.str(version),
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
      librdkafkaShimId,
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
      librdkafkaShimId,
      s"librdkafka $librdkafkaVersion for xkafka on Node.js, prebuilt for each supported platform",
      "MIT",
      "main"                 -> JSON.str("index.js"),
      "files"                -> JSON.arr(JSON.str("index.js")),
      "optionalDependencies" -> JSON.objStr(platforms.map { case (platform, version) => s"@xkafka/librdkafka-$platform" -> version })
    )
  )
}

/** Downloads the pinned rocksdbjni jar from Maven Central once, and verifies it. */
def fetchRocksdb(log: Logger): File = {
  val jar      = rocksdbRoot / s"rocksdbjni-$rocksdbVersion.jar"
  val download = URI.create(s"https://repo1.maven.org/maven2/org/rocksdb/rocksdbjni/$rocksdbVersion/rocksdbjni-$rocksdbVersion.jar").toURL

  IO.createDirectory(rocksdbRoot)
  if (!jar.isFile) {
    log.info(s"Downloading rocksdbjni $rocksdbVersion")
    // Written beside the jar and renamed into place, so a build running at the same time never reads half of it.
    val partial = java.io.File.createTempFile("rocksdbjni", ".part", rocksdbRoot)
    val input   = download.openStream()
    try IO.transfer(input, partial)
    finally input.close()
    IO.move(partial, jar)
  }

  val actualSha256 = MessageDigest.getInstance("SHA-256").digest(IO.readBytes(jar)).iterator.map(byte => f"${byte & 0xff}%02x").mkString
  if (actualSha256 != rocksdbSha256)
    sys.error(s"rocksdbjni checksum mismatch: expected $rocksdbSha256, got $actualSha256")
  jar
}

// The library rocksdbjni carries for each platform the npm packages are built for, by its name in the jar.
val rocksdbLibraries = Map(
  "darwin-arm64"     -> "librocksdbjni-osx-arm64.jnilib",
  "darwin-x64"       -> "librocksdbjni-osx-x86_64.jnilib",
  "linux-x64-gnu"    -> "librocksdbjni-linux64.so",
  "linux-arm64-gnu"  -> "librocksdbjni-linux-aarch64.so",
  "linux-x64-musl"   -> "librocksdbjni-linux64-musl.so",
  "linux-arm64-musl" -> "librocksdbjni-linux-aarch64-musl.so"
)

/** The RocksDB library of one platform, taken from the verified jar the first time it is asked for. */
def rocksdbLibrary(log: Logger, platform: String): File = {
  val name    = rocksdbLibraries.getOrElse(platform, sys.error(s"rocksdbjni carries no library for $platform"))
  val library = rocksdbRoot / platform / name
  if (!library.isFile) {
    val jar = fetchRocksdb(log)
    IO.withTemporaryDirectory(
      staging => {
        IO.unzip(jar, staging, new ExactFilter(name)): Unit
        IO.move(staging / name, library)
      },
      keepDirectory = false
    )
  }
  library
}

// The C shim through which both the JavaScript and the Native backend reach RocksDB.
val rocksdbShim = repositoryRoot / "modules" / "client" / "native" / "src" / "main" / "resources" / "scala-native" / "xkafka_rocksdb.c"

// The shim's identity: the RocksDB release and everything the npm packages are made from. It versions the packages and the shim reports it, so
// the JavaScript backend refuses a shim built from anything else, as it does librdkafka's.
val rocksdbShimId = {
  val digest = MessageDigest.getInstance("SHA-256")
  val inputs = Seq(
    rocksdbShim,
    repositoryRoot / "scripts" / "build-rocksdb-library.sh",
    repositoryRoot / "modules" / "client" / "js" / "npm" / "rocksdb" / "index.js",
    repositoryRoot / "modules" / "client" / "js" / "npm" / "rocksdb" / "LICENSES.txt"
  )
  inputs.foreach(source => digest.update(IO.readBytes(source)))
  s"$rocksdbVersion-${digest.digest().iterator.map(byte => f"${byte & 0xff}%02x").mkString.take(12)}"
}

/** The command that builds the RocksDB shim into the shared library the npm packages carry. */
def rocksdbLibraryCommand(output: File): Seq[String] =
  Seq((repositoryRoot / "scripts" / "build-rocksdb-library.sh").getAbsolutePath, output.getAbsolutePath, rocksdbShimId, rocksdbShim.getAbsolutePath)

/** Writes the RocksDB package of one platform, holding the shim, the RocksDB library and their licenses, and answers its directory. */
def writeRocksdbPlatformPackage(root: File, platform: String, shim: File, library: File): File = {
  import scalajsbundler.util.JSON
  val label     = npmPlatforms.toMap.getOrElse(platform, sys.error(s"no npm package is built for $platform"))
  val os        = platform.split('-')(0)
  val cpu       = platform.split('-')(1)
  val libc      = platform.split('-').lift(2).map(value => if (value == "gnu") "glibc" else value)
  val shimName  = if (os == "darwin") "libxkafka_rocksdb.dylib" else "libxkafka_rocksdb.so"
  val directory = root / s"rocksdb-$platform"
  val licenses  = repositoryRoot / "modules" / "client" / "js" / "npm" / "rocksdb" / "LICENSES.txt"

  IO.delete(directory)
  IO.copyFile(shim, directory / shimName)
  IO.copyFile(library, directory / library.getName)
  IO.write(directory / "LICENSES.txt", s"==== xkafka shim (MIT) ====\n\n${IO.read(repositoryRoot / "LICENSE")}\n\n${IO.read(licenses)}")
  IO.write(
    directory / "package.json",
    npmPackageJson(
      s"@xkafka/rocksdb-$platform",
      rocksdbShimId,
      s"RocksDB $rocksdbVersion from rocksdbjni, with the shim xkafka opens it through, on $label",
      "MIT AND Apache-2.0 AND BSD-3-Clause AND BSD-2-Clause AND Zlib AND bzip2-1.0.6",
      Seq("os" -> JSON.arr(JSON.str(os)), "cpu" -> JSON.arr(JSON.str(cpu))) ++
        libc.map(value => "libc" -> JSON.arr(JSON.str(value))) :+
        ("files" -> JSON.arr(JSON.str(shimName), JSON.str(library.getName), JSON.str("LICENSES.txt"))): _*
    )
  )
  directory
}

/** Writes the RocksDB umbrella package, which depends on each platform package, at the versions given, and finds the one npm installed. */
def writeRocksdbUmbrellaPackage(directory: File, clientBase: File, platforms: Seq[(String, String)]): Unit = {
  import scalajsbundler.util.JSON
  IO.delete(directory)
  IO.copyFile(clientBase / "npm" / "rocksdb" / "index.js", directory / "index.js")
  IO.write(
    directory / "package.json",
    npmPackageJson(
      "@xkafka/rocksdb",
      rocksdbShimId,
      s"RocksDB $rocksdbVersion for xkafka on Node.js, prebuilt for each supported platform",
      "MIT",
      "main"                 -> JSON.str("index.js"),
      "files"                -> JSON.arr(JSON.str("index.js")),
      "optionalDependencies" -> JSON.objStr(platforms.map { case (platform, version) => s"@xkafka/rocksdb-$platform" -> version })
    )
  )
}

/** Removes the tests' installed @xkafka packages, and the npm locks that name them, when `name` was installed from another identity. */
def forgetStalePackage(workspace: File, clientBase: File, name: String, identity: String): Unit = {
  val locks = Seq(clientBase / "package-lock.json", workspace / "package-lock.json", workspace / "node_modules" / ".package-lock.json")
  def installed(lock: File): Option[String] =
    (play.api.libs.json.Json.parse(IO.read(lock)) \ "packages" \ s"node_modules/@xkafka/$name" \ "resolved").asOpt[String]
  if (locks.exists(lock => lock.isFile && installed(lock).exists(!_.contains(s"/local/$identity/"))))
    IO.delete(locks :+ (workspace / "node_modules" / "@xkafka"))
}

val commonSettings = Seq(
  headerLicense := Some(HeaderLicense.MIT("2026", "xkafka contributors")),
  scalacOptions += "-Wnonunit-statement",
  Compile / packageBin / mappings += (repositoryRoot / "LICENSE" -> "META-INF/LICENSE"),
  libraryDependencies ++= Seq(
    "org.typelevel" %%% "cats-effect"         % catsEffectVersion,
    "org.typelevel" %%% "cats-tagless-core"   % catsTaglessVersion,
    "co.fs2"        %%% "fs2-core"            % fs2Version,
    "org.scalameta" %%% "munit"               % munitVersion           % Test,
    "org.typelevel" %%% "munit-cats-effect"   % munitCatsEffectVersion % Test,
    "org.typelevel" %%% "cats-effect-testkit" % catsEffectVersion      % Test,
    "org.scalameta" %%% "munit-scalacheck"    % munitScalacheckVersion % Test,
    "org.typelevel" %%% "cats-laws"           % catsLawsVersion        % Test,
    "org.typelevel" %%% "discipline-munit"    % disciplineMunitVersion % Test
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

lazy val schemaRegistry = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/schema-registry"))
  .dependsOn(kernel)
  .settings(commonSettings)
  .settings(
    name        := "xkafka-schema-registry",
    description := "Serdes that keep their schemas in a schema registry, for xkafka",
    libraryDependencies ++= Seq("io.circe" %%% "circe-core" % circeVersion, "io.circe" %%% "circe-parser" % circeVersion)
  )

// The registry is reached through an HTTP client the application chooses; this module adapts http4s's.
lazy val schemaRegistryHttp4s = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .in(file("modules/schema-registry-http4s"))
  .dependsOn(schemaRegistry)
  .settings(commonSettings)
  .settings(
    name                                 := "xkafka-schema-registry-http4s",
    description                          := "Reaches a schema registry through an http4s client, for xkafka",
    libraryDependencies += "org.http4s" %%% "http4s-client" % http4sVersion
  )
  // The broker tests reach a Confluent registry through Ember, which on Native needs s2n-tls, a library Ubuntu does not package, so they run on
  // the JVM and JavaScript.
  .platformsSettings(JVMPlatform, JSPlatform)(libraryDependencies += "org.http4s" %%% "http4s-ember-client" % http4sVersion % Test)
  // Ember imports Node's own modules.
  .jsSettings(scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule)))

lazy val client = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .in(file("modules/client"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(kernel)
  .settings(commonSettings)
  .settings(
    name        := "xkafka-client",
    description := "Cross-platform functional Kafka client for Scala",
    Compile / packageBin / mappings += (repositoryRoot / "THIRD_PARTY_NOTICES.md" -> "META-INF/THIRD_PARTY_NOTICES.md"),
    // The release the group connection names to the broker.
    buildInfoPackage := "xkafka.internal",
    buildInfoKeys    := Seq[BuildInfoKey](version, "rocksdb" -> rocksdbVersion),
    buildInfoOptions ++= Seq(BuildInfoOption.PackagePrivate, BuildInfoOption.ConstantValue)
  )
  .jsConfigure(
    _.enablePlugins(ScalaJSBundlerPlugin)
  )
  .jsSettings(
    Compile / npmDependencies ++= Seq("koffi" -> koffiVersion, "@xkafka/librdkafka" -> librdkafkaShimId, "@xkafka/rocksdb" -> rocksdbShimId),
    Compile / additionalNpmConfig += "license" -> scalajsbundler.util.JSON.str("MIT"),
    // koffi's install script only compiles koffi from source where no prebuilt binary matches the platform, and every platform xkafka
    // supports has one, so the script is declined.
    Compile / additionalNpmConfig += "allowScripts" -> scalajsbundler.util.JSON.obj(s"koffi@$koffiVersion" -> scalajsbundler.util.JSON.bool(false)),
    Test / additionalNpmConfig                      := (Compile / additionalNpmConfig).value,
    // The build script reuses a build it finds, so each librdkafka release is built in a directory of its own.
    librdkafkaLibraryBuild := target.value / s"librdkafka-shared-build-$librdkafkaVersion",
    librdkafkaMuslBuild    := target.value / s"librdkafka-musl-build-$librdkafkaVersion",
    npmLibrdkafkaPackages  := target.value / "npm",
    npmLibrdkafkaLocal     := npmLibrdkafkaPackages.value / "local" / librdkafkaShimId / "librdkafka",
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
    forgetStaleLibrdkafka := forgetStalePackage((Test / npmUpdate / crossTarget).value, baseDirectory.value, "librdkafka", librdkafkaShimId),
    prepareRocksdb        := rocksdbLibrary(streams.value.log, hostNpmPlatform),
    // The shim alone, which opens the RocksDB library rocksdbjni carries, so it builds in moments.
    sharedRocksdb := {
      val log    = streams.value.log
      val output = target.value / "xkafka-rocksdb" / (if (isMacOs) "libxkafka_rocksdb.dylib" else "libxkafka_rocksdb.so")
      val built  = target.value / "xkafka-rocksdb" / ".shim-id"
      if (!output.isFile || !built.isFile || IO.read(built) != rocksdbShimId) {
        runCommand(log, rocksdbLibraryCommand(output), repositoryRoot)
        IO.write(built, rocksdbShimId)
      }
      output
    },
    npmRocksdbLocal := npmLibrdkafkaPackages.value / "local" / rocksdbShimId / "rocksdb",
    // As for librdkafka: the umbrella package as npm gets it, and a copy the tests install, which names this platform's package by path.
    npmRocksdb := {
      val root     = npmLibrdkafkaPackages.value
      val platform = hostNpmPlatform
      val packaged = writeRocksdbPlatformPackage(root, platform, sharedRocksdb.value, prepareRocksdb.value)
      writeRocksdbUmbrellaPackage(root / "rocksdb", baseDirectory.value, npmPlatforms.map { case (name, _) => name -> rocksdbShimId })
      writeRocksdbUmbrellaPackage(npmRocksdbLocal.value, baseDirectory.value, Seq(platform -> s"file:${packaged.getAbsolutePath}"))
      root
    },
    // The check opens a store with the musl RocksDB library in the container, which shows both it and the shim work with musl.
    npmRocksdbMusl := {
      val log      = streams.value.log
      val platform = s"linux-${hostNpmPlatform.split('-')(1)}-musl" // The container runs on this machine's architecture.
      val output   = target.value / "xkafka-rocksdb-musl" / "libxkafka_rocksdb.so"
      val library  = rocksdbLibrary(log, platform)
      val project  = repositoryRoot.getAbsolutePath
      val check    =
        s"mkdir -p /tmp/koffi && npm install --silent --no-audit --no-fund --prefix /tmp/koffi koffi@$koffiVersion && " +
          s"node ${(repositoryRoot / "scripts" / "check-rocksdb-library.cjs").getAbsolutePath} /tmp/koffi ${output.getAbsolutePath} " +
          s"${library.getAbsolutePath} $rocksdbShimId $rocksdbVersion"
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
          s"apk add --no-cache build-base >/dev/null && ${rocksdbLibraryCommand(output).mkString(" ")} && $check"
        ),
        repositoryRoot
      )
      writeRocksdbPlatformPackage(npmLibrdkafkaPackages.value, platform, output, library)
    },
    forgetStaleRocksdb := forgetStalePackage((Test / npmUpdate / crossTarget).value, baseDirectory.value, "rocksdb", rocksdbShimId),
    rocksdbShimTest    := {
      val script = (repositoryRoot / "scripts" / "rocksdb-shim-test.sh").getAbsolutePath
      runCommand(
        streams.value.log,
        Seq(script, sharedRocksdb.value.getAbsolutePath, prepareRocksdb.value.getAbsolutePath, rocksdbVersion, rocksdbShimId),
        repositoryRoot
      )
    },
    // The tests ask for the local copies beside the published versions, and the resolutions settle those conflicts in their favour.
    Test / npmResolutions := Map(
      "@xkafka/librdkafka" -> s"file:${npmLibrdkafkaLocal.value.getAbsolutePath}",
      "@xkafka/rocksdb"    -> s"file:${npmRocksdbLocal.value.getAbsolutePath}"
    ),
    Test / npmDependencies ++= (Test / npmResolutions).value.toSeq,
    // npm links a local directory without installing what it depends on, so the local copies are installed as packages instead.
    Test / npmExtraArgs ++= Seq("--install-links"),
    // The published jar lists its npm dependencies for every configuration, and an application's tests install a library's test dependencies too,
    // so the local copy above is taken out of the list the jar carries.
    Compile / packageBin / mappings := {
      import scalajsbundler.NpmDependencies
      val manifest = NpmDependencies.manifestFileName
      val locals   = (Test / npmResolutions).value.toSet
      (Compile / packageBin / mappings).value.map {
        case (file, `manifest`) =>
          val listed    = play.api.libs.json.Json.parse(IO.read(file)).as(NpmDependencies.serializer)
          val published = listed.copy(testDependencies = listed.testDependencies.filterNot(locals))
          NpmDependencies.writeManifest(published, target.value / "published-npm-dependencies") -> manifest
        case other => other
      }
    },
    // npm keeps an installed local package, through its locks, even after the path it is asked for changes, so one from an earlier library
    // identity is removed along with the locks that name it.
    Test / npmUpdate := (Test / npmUpdate).dependsOn(npmLibrdkafka, forgetStaleLibrdkafka, npmRocksdb, forgetStaleRocksdb).value,
    // The identities of the libraries this build was made with, which the loaded libraries must report, and the RocksDB release.
    buildInfoKeys ++= Seq[BuildInfoKey]("shim" -> librdkafkaShimId, "rocksdbShim" -> rocksdbShimId)
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
    prepareRocksdb := rocksdbLibrary(streams.value.log, hostNpmPlatform),
    // A Native program finds the RocksDB library where XKAFKA_ROCKSDB_LIBRARY says, so the tests name the one extracted for this platform.
    Test / envVars += "XKAFKA_ROCKSDB_LIBRARY" -> prepareRocksdb.value.getAbsolutePath,
    Compile / nativeLink                       := (Compile / nativeLink).dependsOn(prepareLibrdkafka).value,
    Test / nativeLink                          := (Test / nativeLink).dependsOn(prepareLibrdkafka).value,
    nativeConfig                               := {
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
      //
      // Scala Native's own search paths include Homebrew's shared directories, where OpenSSL 4 is, so these prefixes come first: the shim then
      // compiles against OpenSSL 3's headers and its own `-lcrypto` finds OpenSSL 3, the release librdkafka was built against.
      val searchPaths = s"-L${(target.value / "librdkafka-static-link").getAbsolutePath}" +:
        prefixes.map(prefix => s"-L${prefix.getAbsolutePath}/lib")
      config
        .withCompileOptions((prefix +: prefixes).map(directory => s"-I${directory.getAbsolutePath}/include") ++ _)
        .withLinkingOptions(options => searchPaths ++ options ++ ("-lrdkafka" +: openSsl) ++ Seq("-lz", "-lzstd"))
    }
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion,
      // Only an application keeping its stores on disk needs RocksDB, and adds it itself.
      "org.rocksdb"       % "rocksdbjni"               % rocksdbVersion      % Optional,
      "at.yawk.lz4"       % "lz4-java"                 % lz4JavaVersion,
      "org.xerial.snappy" % "snappy-java"              % snappyJavaVersion,
      "org.slf4j"         % "slf4j-nop"                % slf4jVersion        % Test,
      "org.apache.kafka"  % "kafka-streams"            % kafkaClientsVersion % Test,
      "org.apache.kafka"  % "kafka-streams-test-utils" % kafkaClientsVersion % Test
    )
  )

// The Kafka Streams application the broker tests run beside xkafka's streams group members; scripts/integration-test.sh starts it.
lazy val streamsJava = project
  .in(file("modules/streams-java"))
  .dependsOn(client.jvm % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name                   := "xkafka-streams-java",
    description            := "Java Kafka Streams' partitioners and processors in xkafka streams programs, on the JVM",
    tlMimaPreviousVersions := Set.empty,
    libraryDependencies ++= Seq("org.apache.kafka" % "kafka-streams" % kafkaClientsVersion)
  )

lazy val streamsFixture = project
  .in(file("modules/streams-fixture"))
  .enablePlugins(NoPublishPlugin)
  .settings(
    headerLicense := Some(HeaderLicense.MIT("2026", "xkafka contributors")),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaClientsVersion,
      "org.slf4j"        % "slf4j-nop"     % slf4jVersion
    ),
    Compile / mainClass := Some("xkafka.fixture.StreamsFixture"),
    fork                := true
  )

lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(client.jvm, streamsJava)
  .settings(
    // The examples of querying stores show an HTTP connection between members, so the site depends on http4s's server and Ember client; of the
    // published modules, only xkafka-schema-registry-http4s depends on http4s, and on its client alone.
    libraryDependencies ++= Seq("ember-server", "ember-client", "dsl").map(module => "org.http4s" %% s"http4s-$module" % http4sVersion),
    tlSiteHelium := tlSiteHelium.value.site.topNavigationBar(
      homeLink = IconLink.external("https://github.com/PMBMegalom/xkafka", HeliumIcon.home)
    )
  )

lazy val root = tlCrossRootProject.aggregate(kernel, client, schemaRegistry, schemaRegistryHttp4s, streamsJava).settings(name := "xkafka")
