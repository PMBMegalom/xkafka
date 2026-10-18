# Platform notes

The API is the same everywhere. These are the places the runtimes differ.

## Record delivery

Every backend has one record source for each consumer: a single poll owned by
the consumer, feeding both the record stream and the assignment. The poll starts
when the consumer is made, so a consumer joins its group and takes its share of
the partitions even before anything reads its records. A partition whose buffer
fills is paused at the broker until its reader catches up, so a slow partition
does not hold up the others.

A consumer which stops reading records for long enough stops seeing rebalances
too, because both come from that one poll.

On the JVM each consumer has a thread of its own, which makes every call to the
Java client.

## Scala Native

The librdkafka the Native backend expects is built with TLS, SASL SCRAM and
OAUTHBEARER, and gzip and zstd compression enabled, so it can reach
authenticated brokers and read and write every compression codec. Snappy and LZ4
are built into librdkafka itself.

librdkafka links these against system libraries, so any application linking the
Native backend needs OpenSSL, zlib, and zstd available:
`libssl-dev`, `zlib1g-dev`, and `libzstd-dev` on Debian and Ubuntu, or
`brew install openssl@3 zstd zlib` on macOS. Homebrew keeps them keg-only, so
on macOS their locations have to be named when linking.

### Linking an application

Published Native artifacts contain the C glue code and request `-lrdkafka`.
Applications with librdkafka installed in standard compiler and linker locations
need no additional xkafka settings. For a nonstandard installation:

```scala
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ :+ "-L/path/to/librdkafka/lib")
```

Linking the static `librdkafka.a` also needs the libraries it was built
against, named in this order:

```scala
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ ++ Seq(
    "-L/path/to/librdkafka/lib", "-lrdkafka", "-lssl", "-lcrypto", "-lz", "-lzstd"
  ))
```

On macOS add a `-L` for the OpenSSL installation as well.

## Scala.js

The backend is librdkafka, the same release and the same C shim Scala Native
uses, called from Node.js through koffi. It targets Node.js rather than
browsers.

The library comes from npm as `@xkafka/librdkafka`, which sbt-scalajs-bundler
installs. npm adds the prebuilt library for the platform it runs on:

| Platform | Package |
| --- | --- |
| macOS on Apple silicon | `@xkafka/librdkafka-darwin-arm64` |
| macOS on Intel | `@xkafka/librdkafka-darwin-x64` |
| Linux x64 with glibc | `@xkafka/librdkafka-linux-x64-gnu` |
| Linux arm64 with glibc | `@xkafka/librdkafka-linux-arm64-gnu` |
| Linux x64 with musl, such as Alpine | `@xkafka/librdkafka-linux-x64-musl` |
| Linux arm64 with musl | `@xkafka/librdkafka-linux-arm64-musl` |

Nothing else needs installing: each library carries its own OpenSSL, zlib and
zstd. Each xkafka release depends on the library built from its own shim, and
refuses to start with any other.

## Transformations

Serializers, deserializers, producer and consumer settings, committable offsets
and records, partition record streams, and consumers have cats-tagless
`FunctorK` instances. `KafkaProducer` has `mapK` but no `FunctorK` instance,
because translating the acknowledgement nested inside the enqueue needs a
`Functor` for the target effect. `KafkaClient.imapK` transforms a complete
client between effects in both directions while preserving `Resource`
cancellation semantics.
