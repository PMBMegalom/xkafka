ThisBuild / scalaVersion := "3.3.8"

val xkafkaVersion = sys.env.getOrElse("XKAFKA_VERSION", sys.error("XKAFKA_VERSION must name the xkafka version to test"))

ThisBuild / resolvers += "Sonatype Central Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

lazy val smoke = crossProject(JVMPlatform, JSPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("."))
  .settings(
    name                := "xkafka-downstream-smoke",
    publish / skip      := true,
    Compile / mainClass := Some("xkafka.downstream.DownstreamSmoke"),
    libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % xkafkaVersion
  )
  .jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin))
  .jsSettings(
    scalaJSUseMainModuleInitializer := true,
    Compile / additionalNpmConfig += "license"      -> scalajsbundler.util.JSON.str("MIT"),
    Compile / additionalNpmConfig += "allowScripts" -> scalajsbundler.util.JSON.obj(
      "@confluentinc/kafka-javascript@1.10.1" -> scalajsbundler.util.JSON.bool(true)
    )
  )
  .jvmSettings(Compile / run / fork := true)
  .nativeSettings(
    nativeConfig := {
      val config = nativeConfig.value
      sys.env.get("XKAFKA_LIBRDKAFKA_PREFIX").fold(config)(prefix =>
        config.withCompileOptions(_ :+ s"-I$prefix/include").withLinkingOptions(_ :+ s"-L$prefix/lib")
      )
    }
  )
