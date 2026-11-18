ThisBuild / scalaVersion := "3.9.0"

// The xkafka release measured, from Maven Central, or one published locally with `sbt publishLocal`.
val xkafkaVersion = sys.env.getOrElse("XKAFKA_VERSION", "0.8.0")

ThisBuild / resolvers += "Sonatype Central Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

lazy val bench = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Full)
  .in(file("."))
  .settings(
    name                := "xkafka-benchmarks",
    publish / skip      := true,
    Compile / mainClass := Some("xkafka.benchmarks.Benchmark"),
    libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % xkafkaVersion
  )
  .jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin))
  .jsSettings(
    scalaJSUseMainModuleInitializer := true,
    // Measured as an application ships, fully optimised: a fast link also turns on cats-effect's tracing, which slows Scala.js down.
    scalaJSStage := FullOptStage,
    Compile / additionalNpmConfig += "license"      -> scalajsbundler.util.JSON.str("MIT"),
    // koffi's install script only compiles koffi where no prebuilt binary matches, which never happens on the platforms xkafka supports.
    Compile / additionalNpmConfig += "allowScripts" -> scalajsbundler.util.JSON.obj("koffi@3.3.3" -> scalajsbundler.util.JSON.bool(false))
  )
  .jvmSettings(Compile / run / fork := true)
  .nativeSettings(
    // Linked against the static librdkafka scripts/benchmark.sh prepares, and the libraries it was built against. On macOS the script names
    // OpenSSL 3 by path, as Homebrew's OpenSSL 4, which the link would otherwise find, lacks the engine API librdkafka calls.
    nativeConfig := {
      val config      = nativeConfig.value.withMode(scala.scalanative.build.Mode.releaseFast)
      val prefix      = sys.env.getOrElse("XKAFKA_LIBRDKAFKA_PREFIX", sys.error("XKAFKA_LIBRDKAFKA_PREFIX must name a librdkafka installation"))
      def words(name: String) = sys.env.getOrElse(name, "").split(' ').toList.filter(_.nonEmpty)
      val openSsl  = words("XKAFKA_OPENSSL_LIBRARIES") match {
        case Nil       => Seq("-lssl", "-lcrypto")
        case libraries => libraries
      }
      config.withCompileOptions(options => (s"-I$prefix/include" +: words("XKAFKA_INCLUDE_PATHS")) ++ options)
        .withLinkingOptions(options => (s"-L$prefix/lib" +: words("XKAFKA_LIBRDKAFKA_SEARCH_PATHS")) ++ options ++ ("-lrdkafka" +: openSsl) ++ Seq("-lz", "-lzstd"))
    }
  )
