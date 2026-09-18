# Third-party notices

xkafka is distributed under the MIT License. Its dependencies remain under
their respective licenses and are not relicensed as part of xkafka.

## Runtime backends

| Component | Version | License | Use |
| --- | --- | --- | --- |
| [fs2-kafka](https://github.com/typelevel/fs2-kafka) | 4.0.0 | MIT | JVM Kafka backend |
| [Confluent Kafka JavaScript](https://github.com/confluentinc/confluent-kafka-javascript) | 1.10.1 | MIT | Scala.js Kafka backend |
| [librdkafka](https://github.com/confluentinc/librdkafka) | 2.15.1 | BSD-2-Clause | Native backend and the native layer used by the JavaScript backend |

The Confluent Kafka JavaScript distribution includes MIT-licensed work derived
from node-rdkafka and KafkaJS. Its bundled librdkafka source also contains
third-party components under permissive licenses; their complete notices are
provided in librdkafka's `LICENSES.txt` and in the license files shipped with
the Confluent package.

The JavaScript dependency graph and its declared licenses are recorded in
`modules/client/js/package-lock.json`. Build-only dependencies are not part of
xkafka's published runtime API.

Published xkafka library artifacts declare or download these dependencies; the
repository does not relicense their source or binaries. Anyone distributing an
application or other binary bundle containing them must retain the applicable
upstream copyright and license notices.
