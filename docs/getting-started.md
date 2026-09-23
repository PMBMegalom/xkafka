# Getting started

## Dependency

```scala
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-client" % "@VERSION@"
```

The `%%%` operator selects the artifact for the platform being built. The same
code then compiles against the JVM, Scala.js, and Scala Native artifacts, and
each brings its own backend.

Two modules are published:

- `xkafka-kernel` holds the portable data model and the producer and consumer
  algebras.
- `xkafka-client` holds `KafkaClient` and the three backends, and depends on the
  kernel.

Most applications need `xkafka-client` alone.

## Constructing a client

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.*

val client = KafkaClient[IO]
```

The selected artifact supplies the backend, so this line is the same on every
platform.

## Scala.js

The backend is `@confluentinc/kafka-javascript`, which is installed through
sbt-scalajs-bundler and runs on Node.js. Use the Node version recorded in
`.nvmrc`:

```sh
nvm use
```

## Scala Native

The backend is librdkafka, reached through a small C shim that ships inside the
published artifact. An application needs librdkafka available to its compiler
and linker. See [Platform notes](platform-notes.md) for the linking recipes,
including the flags a static link requires.
