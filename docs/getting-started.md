# Getting started

## Dependency

```scala
// Add xkafka to your build; %%% picks the artifact for your platform
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % "@VERSION@"
```

The same code compiles for the JVM, Scala.js and Scala Native, and each
runs on its platform's Kafka client.

## Modules

Most applications need `xkafka-client` only. The other modules are:

- `xkafka-kernel` holds the data types, serializers, deserializers and serdes,
  the producer, consumer, and administration interfaces, and the streams DSL,
  and connects to nothing itself. `xkafka-client` depends on it, so you only
  add it yourself for code that must not depend on a client.
- `xkafka-avro`, `xkafka-json-schema` and `xkafka-protobuf` write and read Avro,
  JSON Schema and Protobuf with the schemas a Confluent Schema Registry keeps.
  Each brings in the registry client, `xkafka-schema-registry`. To reach the
  registry through http4s, also add `xkafka-schema-registry-http4s` and an
  http4s client such as Ember. See [Schema registry](schema-registry.md).
- `xkafka-protobuf-scalapb` writes and reads the Protobuf messages
  [ScalaPB](https://scalapb.github.io) generates, with the registry's schemas.
  See [ScalaPB messages](other-libraries.md#scalapb-messages).
- `xkafka-smithy4s` writes and reads the types smithy4s generates from Smithy
  files, as smithy4s's JSON or Protobuf, with the registry's schemas. See
  [smithy4s types as JSON](other-libraries.md#smithy4s-types-as-json).
- `xkafka-avro-vulcan` and `xkafka-avro4s` write and read the types vulcan and
  avro4s have codecs for, with the registry's schemas, on the JVM only. See
  [vulcan or avro4s codecs](other-libraries.md#vulcan-or-avro4s-codecs).
- `sbt-xkafka-codegen` generates Scala classes from Avro and JSON Schema files
  while sbt compiles; `xkafka-codegen` is the generator it runs. See
  [Generating classes from schema files](code-generation.md).
- `xkafka-streams-http4s` lets the members of a streams application query each
  other's stores over http4s. See [Queries](streams/queries.md).
- `xkafka-streams-java`, on the JVM only, lets streams programs use code written
  for Java Kafka Streams, such as its partitioners. It depends on Java Kafka
  Streams itself. See [Java Kafka Streams](streams/java-kafka-streams.md).

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

What each platform needs at run time is described below. Once your project
builds, [Producing](producing.md) and [Consuming](consuming.md) show how to
write and read records, and [Settings](settings.md) covers what you can
configure.

## JVM

Nothing else is needed. xkafka brings in the Apache Kafka Java client, and runs
on Java 17 or newer.

## Scala.js

On Scala.js, xkafka runs on Node.js 24, not in browsers. It uses librdkafka from
the npm package `@xkafka/librdkafka`, which ships a prebuilt library for each
platform it supports, so you install nothing else. The sbt-scalajs-bundler
plugin installs the package. Add it, and enable it in your Scala.js project:

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

On Scala Native, xkafka links librdkafka 2.16.0. Install it where your
compiler and linker can find it. [Platform notes](platform-notes.md) gives the
linking recipes, including the flags a static link needs.
