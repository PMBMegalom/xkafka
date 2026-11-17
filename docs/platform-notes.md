# Platform notes

The API is the same everywhere. These notes cover what each platform needs, and
how the API carries across platforms and effects.

## Consumers

On the JVM each consumer uses one thread of its own. Everything else about how a
consumer joins its group and reads records is the same on every platform; see
[Consuming](consuming.md).

## Scala Native

Scala Native needs librdkafka 2.16.0, the release xkafka is built and tested
with. An application does not compile against an older one, such as Ubuntu
24.04's own librdkafka 2.3.0.
Confluent publishes each release for Debian and Ubuntu. On Ubuntu 24.04:

```sh
# Install librdkafka 2.16.0 from Confluent's packages on Ubuntu 24.04
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://packages.confluent.io/clients/deb/archive.key | sudo gpg --dearmor -o /etc/apt/keyrings/confluent-clients.gpg
echo "deb [signed-by=/etc/apt/keyrings/confluent-clients.gpg] https://packages.confluent.io/clients/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/confluent-clients.list
sudo apt-get update
sudo apt-get install librdkafka-dev=2.16.0-1.cflt~ubu24 librdkafka1=2.16.0-1.cflt~ubu24  # Install exactly this release
```

Use a librdkafka built with TLS, SASL SCRAM and every compression codec, so it
can reach authenticated brokers and read and write records in any compression.

librdkafka uses the system's OpenSSL, zlib and zstd, so install those too:
`libssl-dev`, `zlib1g-dev`, and `libzstd-dev` on Debian and Ubuntu, or
`brew install openssl@3 zstd zlib` on macOS.

### Linking an application

Compiling an application needs the headers of librdkafka and OpenSSL, and xkafka
asks the linker for `-lrdkafka`, `-lssl` and `-lcrypto`. With both installed where the
compiler and linker look by default, no further settings are needed. For an
installation elsewhere:

```scala
// Tell the compiler and the linker where librdkafka is installed
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ :+ "-L/path/to/librdkafka/lib")
```

Linking the static `librdkafka.a` also needs the libraries it was built
against, named in this order:

```scala
// Link the static librdkafka, with the libraries it was built against, in this order
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ ++ Seq(
    "-L/path/to/librdkafka/lib", "-lrdkafka", "-lssl", "-lcrypto", "-lz", "-lzstd"
  ))
```

On macOS, Homebrew's libraries are not where the linker looks by default, so a
static link there also names their directories, ahead of the libraries:

```scala
// Link the static librdkafka on macOS, with Homebrew's OpenSSL 3, zstd and zlib
nativeConfig := nativeConfig.value
  .withCompileOptions(_ :+ "-I/path/to/librdkafka/include")
  .withLinkingOptions(_ ++ Seq(
    "-L/path/to/librdkafka/lib",
    "-L/opt/homebrew/opt/openssl@3/lib", "-L/opt/homebrew/opt/zstd/lib", "-L/opt/homebrew/opt/zlib/lib",
    "-lrdkafka", "-lssl", "-lcrypto", "-lz", "-lzstd"
  ))
```

`brew --prefix openssl@3`, `brew --prefix zstd` and `brew --prefix zlib` print
the directories to use, without the `/lib`, if Homebrew is installed elsewhere.

### Stores on disk

An application that keeps its streams stores on disk, with a
[state directory](streams/running.md#keeping-stores-on-disk), also needs RocksDB
10.10.1 when it runs. Take it from the `org.rocksdb:rocksdbjni:10.10.1.1` jar on
Maven Central, which carries it for each platform:

| Platform | File in the jar |
| --- | --- |
| macOS on Apple silicon | `librocksdbjni-osx-arm64.jnilib` |
| macOS on Intel | `librocksdbjni-osx-x86_64.jnilib` |
| Linux x64 with glibc | `librocksdbjni-linux64.so` |
| Linux arm64 with glibc | `librocksdbjni-linux-aarch64.so` |
| Linux x64 with musl, such as Alpine | `librocksdbjni-linux64-musl.so` |
| Linux arm64 with musl | `librocksdbjni-linux-aarch64-musl.so` |

```sh
# Put this platform's RocksDB next to the application, here for Linux x64 with glibc
unzip -j rocksdbjni-10.10.1.1.jar librocksdbjni-linux64.so -d path/to/app/bin
```

The application looks for the file in the directory of its executable, then
wherever the system looks for libraries. To keep it somewhere else, set
`XKAFKA_ROCKSDB_LIBRARY` to its path. Any other RocksDB release is refused when
the application opens its first store. Nothing more is needed to compile or
link.

### Schema registry

Reaching a schema registry through http4s's Ember client also needs the s2n-tls
library. See [Schema registry](schema-registry.md#dependencies).

## Scala.js

xkafka uses librdkafka here, the same release as on Scala Native. It runs on
Node.js 24, not in browsers.

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
zstd. Stores kept on disk use RocksDB, which comes the same way, from
`@xkafka/rocksdb` and its package for the platform, such as
`@xkafka/rocksdb-darwin-arm64`. Each xkafka release needs the exact `@xkafka/librdkafka` version it was
published with, so do not pin or override that package. With any other version,
acquiring a client fails with a `BackendFailure` naming both versions.

## Changing the effect type

These have cats-tagless `FunctorK` instances, and a `mapK` that changes their
effect: serializers, deserializers and serdes; producer, transactional producer
and consumer settings; offset committers, committable offsets, batches and
records; partition record streams; consumers; administration clients; and the
schema registry client.

`KafkaProducer` has `mapK` too, which needs a `Functor` for the target effect,
and so no `FunctorK` instance.

Types that both run effects and take them from you change effect with `imapK`,
which takes a transformation each way, and have a cats-tagless `InvariantK`
instance:
`Transaction`, `KafkaTransactionalProducer`, a streams `Pipeline` or
`Topology`, where a [processor](streams/processors.md) runs its handler in one
effect and its stores in the other, and `KafkaStreams`, whose queries go both
ways.

`KafkaClient.imapK` changes the effect of a whole client, given a transformation
each way, and its resources are still released when cancelled.
