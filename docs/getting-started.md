# Getting started

## Dependency

```scala
// Add xkafka to your build; %%% picks the artifact for your platform
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % "@VERSION@"
```

The `%%%` operator selects the artifact for the platform being built. The same
code then compiles against the JVM, Scala.js, and Scala Native artifacts, and
each one runs on its platform's Kafka client.

The modules are:

- `xkafka-kernel` holds the data types, the producer, consumer, and
  administration interfaces, and the streams DSL, and connects to nothing itself.
- `xkafka-client` holds `KafkaClient`, which connects to Kafka on each platform,
  and depends on the kernel.
- `xkafka-schema-registry`, `xkafka-schema-registry-http4s` and `xkafka-avro`
  write and read Avro with the schemas a Confluent Schema Registry keeps. See
  [Schema registry](schema-registry.md).

Most applications need `xkafka-client` alone. On the JVM, `xkafka-streams-java` lets
streams programs use code written for Java Kafka Streams, such as its partitioners;
it depends on Java Kafka Streams itself.

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
described below.

## Scala.js

xkafka uses librdkafka here, the same release as on Scala Native, and runs on
Node.js 24. It comes from npm as `@xkafka/librdkafka`, which brings a prebuilt
library for the platform it runs on. sbt-scalajs-bundler installs it, so enable
that plugin in the Scala.js project:

```scala
// In project/plugins.sbt: add the plugin that installs the npm package
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % "0.21.1")
```

```scala
// In build.sbt: enable that plugin for the Scala.js project
.jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin))
```

## Scala Native

xkafka uses librdkafka here, which must be installed where the application's
compiler and linker can find it. See [Platform notes](platform-notes.md) for the linking recipes,
including the flags a static link requires.
