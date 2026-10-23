# Getting started

## Dependency

```scala
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % "@VERSION@"
```

The `%%%` operator selects the artifact for the platform being built. The same
code then compiles against the JVM, Scala.js, and Scala Native artifacts, and
each one runs on its platform's Kafka client.

Two modules are published:

- `xkafka-kernel` holds the data types and the producer, consumer, and
  administration interfaces, with no backend.
- `xkafka-client` holds `KafkaClient` and the three backends, and depends on the
  kernel.

Most applications need `xkafka-client` alone.

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
import cats.effect.IO

import xkafka.*

val client = KafkaClient[IO]
```

This line is the same on every platform. What each platform needs at runtime is
described below.

## Scala.js

The backend is librdkafka, the same release Scala Native uses, and it runs on
Node.js 24. It comes from npm as `@xkafka/librdkafka`, which brings a prebuilt
library for the platform it runs on. sbt-scalajs-bundler installs it, so enable
that plugin in the Scala.js project:

```scala
// project/plugins.sbt
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % "0.21.1")
```

```scala
// build.sbt
.jsConfigure(_.enablePlugins(ScalaJSBundlerPlugin))
```

## Scala Native

The backend is librdkafka, which must be installed where the application's
compiler and linker can find it. See [Platform notes](platform-notes.md) for the linking recipes,
including the flags a static link requires.
