# Platform notes

The API is the same everywhere. These are the places the runtimes differ.

## Record delivery

On the JVM the partition streams come from fs2-kafka and are fed independently.
On Scala.js and Scala Native they share one record source: a single poll owned
by the consumer, feeding both the record stream and the assignment. A partition
whose buffer fills is paused at the broker until its reader catches up, so a
slow partition does not hold up the others.

A consumer which stops reading records for long enough stops seeing rebalances
too, because both come from that one poll.

## Scala Native

The librdkafka the Native backend expects is built with TLS, SASL SCRAM and
OAUTHBEARER, and gzip and zstd compression enabled, so it can reach
authenticated brokers and read compressed topics.

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

The backend is `@confluentinc/kafka-javascript`, reached through its librdkafka
configuration keys. It targets Node.js rather than browsers.

## Transformations

Serializers, deserializers, producer and consumer settings, committable offsets
and records, partition record streams, and consumers have cats-tagless
`FunctorK` instances. `KafkaProducer` has `mapK` but no `FunctorK` instance,
because translating the acknowledgement nested inside the enqueue needs a
`Functor` for the target effect. `KafkaClient.imapK` transforms a complete
client between effects in both directions while preserving `Resource`
cancellation semantics.
