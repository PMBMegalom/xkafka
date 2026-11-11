# Getting started

## Dependency

```scala
// Add xkafka to your build; %%% picks the artifact for your platform
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % "@VERSION@"
```

The `%%%` operator selects the artifact for the platform being built. The same
code then compiles against the JVM, Scala.js, and Scala Native artifacts, and
each one runs on its platform's Kafka client.

Most applications need `xkafka-client` alone. The other modules are:

- `xkafka-kernel` holds the data types, serializers, deserializers and serdes,
  the producer, consumer, and administration interfaces, and the streams DSL,
  and connects to nothing itself. `xkafka-client` depends on it, so you only
  add it yourself for code that must not depend on a client.
- `xkafka-avro`, `xkafka-json-schema` and `xkafka-protobuf` write and read Avro,
  JSON Schema and Protobuf with the schemas a Confluent Schema Registry keeps.
  Each brings in `xkafka-schema-registry`, the registry client, and
  `xkafka-schema-registry-http4s` lets it reach the registry through an http4s
  client.
  See [Schema registry](schema-registry.md).
- `xkafka-streams-http4s` lets the members of a streams application query each
  other's stores over http4s. See [Queries](streams/queries.md).
- `xkafka-streams-java`, on the JVM only, lets streams programs use code written
  for Java Kafka Streams, such as its partitioners. It depends on Java Kafka
  Streams itself.

## Versioning

xkafka follows early semantic versioning.

Within a series, upgrades keep binary compatibility. Code built against `0.1.0`
runs against `0.1.1` without recompiling.

Across a series, upgrades may break compatibility, in both binary and source
form. Going from `0.1.x` to `0.2.0` can mean recompiling, and changing code.

After `1.0.0`, that boundary moves to the major version, and every `1.x.y` will
stay binary compatible with `1.0.0`.

While the major version is `0`, expect the API to change between series.

If another dependency brings in a version across one of these boundaries, sbt
warns about it.

## Constructing a client

```scala mdoc:compile-only
// Construct the client, in the same way on every platform
import cats.effect.IO

import xkafka.*

val client = KafkaClient[IO]
```

This line is the same on every platform. What each platform needs at runtime is
described below. Once it builds, [Producing](producing.md) and
[Consuming](consuming.md) show how to write and read records, and
[Settings](settings.md) covers what you can configure.

## Scala.js

xkafka uses librdkafka here, the same release as on Scala Native, and runs on
Node.js 24, not in browsers. It comes from npm as `@xkafka/librdkafka`, with a
prebuilt library for the platform it runs on, so nothing else needs installing.
sbt-scalajs-bundler installs the npm package, so enable that plugin in the
Scala.js project:

```scala
// In project/plugins.sbt: add the plugin that installs the npm package
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % "0.21.1")
```

```scala
// In build.sbt: enable that plugin for the Scala.js side of a cross project
.jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin))
```

In a project that builds for Scala.js only, call `.enablePlugins(ScalaJSBundlerPlugin)`
on it instead. See [Platform notes](platform-notes.md) for the packages
npm installs.

## Scala Native

xkafka uses librdkafka 2.16.0 here, which must be installed where the
application's compiler and linker can find it. See [Platform notes](platform-notes.md) for the linking recipes,
including the flags a static link requires.
