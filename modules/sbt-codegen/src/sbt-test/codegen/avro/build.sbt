scalaVersion := "3.3.9"

libraryDependencies += "io.github.pmbmegalom" %% "xkafka-avro" % sys.props("plugin.version")

// A project the plugin generates from another directory of schemas.
lazy val elsewhere = project
  .settings(
    scalaVersion := "3.3.9",
    libraryDependencies += "io.github.pmbmegalom" %% "xkafka-avro" % sys.props("plugin.version"),
    Compile / xkafkaAvroSources := Seq(baseDirectory.value / "schemas")
  )
