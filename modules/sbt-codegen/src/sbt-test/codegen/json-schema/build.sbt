scalaVersion := "3.3.9"

libraryDependencies += "io.github.pmbmegalom" %% "xkafka-json-schema" % sys.props("plugin.version")

xkafkaJsonSchemaPackage := Some("example.untitled")
