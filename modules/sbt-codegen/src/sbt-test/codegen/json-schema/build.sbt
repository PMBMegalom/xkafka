scalaVersion := "3.9.0"

libraryDependencies += "io.github.pmbmegalom" %% "xkafka-json-schema" % sys.props("plugin.version")

xkafkaJsonSchemaPackage := Some("example.untitled")
