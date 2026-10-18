# Third-party notices

xkafka is distributed under the MIT License. Its dependencies remain under
their respective licenses and are not relicensed as part of xkafka.

## Runtime backends

| Component | Version | License | Use |
| --- | --- | --- | --- |
| [Apache Kafka Java client](https://kafka.apache.org) (`kafka-clients`) | 4.4.0 | Apache-2.0 | JVM Kafka backend |
| [lz4-java](https://github.com/yawkat/lz4-java) | 1.11.4 | Apache-2.0 | LZ4 compression for the JVM backend |
| [librdkafka](https://github.com/confluentinc/librdkafka) | 2.15.1 | BSD-2-Clause | Kafka backend for Scala Native and Scala.js |
| [koffi](https://koffi.dev) | 3.3.3 | MIT | Calls librdkafka from Node.js for the Scala.js backend |
| [OpenSSL](https://www.openssl.org) | 3.5.7 | Apache-2.0 | TLS and SASL in the prebuilt Scala.js libraries |
| [Zstandard](https://github.com/facebook/zstd) | 1.5.7 | BSD-3-Clause | Compression in the prebuilt Scala.js libraries |
| [zlib](https://zlib.net) | 1.3.2 | Zlib | Compression in the prebuilt Scala.js libraries |

The `@xkafka/librdkafka-*` npm packages hold librdkafka with OpenSSL, zlib and
zstd linked in. Each package's `LICENSES.txt` carries the notices of everything
in its library, including the third-party components librdkafka bundles.

On Node.js, xkafka depends only on koffi, which brings its own prebuilt binary
for the platform, and on `@xkafka/librdkafka`. The npm tooling the build itself
uses, such as webpack, is not part of xkafka's published runtime API.

Published xkafka library artifacts declare or download these dependencies; the
repository does not relicense their source or binaries. Anyone distributing an
application or other binary bundle containing them must retain the applicable
upstream copyright and license notices.
