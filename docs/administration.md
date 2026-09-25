# Administration

`KafkaClient[F].admin` is a `Resource` over the topic operations. It reads and changes cluster
metadata only, so it neither produces nor consumes.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    newTopic <- NewTopic.from(topic, partitions = 6, replicationFactor = 3).liftTo[IO]
    _        <- KafkaClient[IO].admin(client).use(_.createTopics(NonEmptySet.one(newTopic)))
  yield ()
```

## What it does

- `createTopics` creates each topic with the partitions, replication factor, and configuration given.
- `deleteTopics` deletes each topic.
- `createPartitions` adds partitions to a topic, which Kafka allows only as an increase.
- `describeTopics` reports the partitions of each requested topic.

`NewTopic.from` rejects a partition count or replication factor that is not positive, so a request
the broker would refuse cannot be built.

@:callout(warning)
Kafka reports an outcome for each topic, and the backends do not agree on whether that detail
survives the trip. These operations therefore report the first failure and nothing more, which is
what every backend can deliver. A request covering several topics may have changed the cluster
before the failure it reports.
@:@

@:callout(info)
`describeTopics` fails where the cluster does not have one of the requested topics. Creating,
growing, and deleting all propagate through the cluster in their own time, so a description taken
immediately afterwards may not agree with the request that preceded it.
@:@

## What it does not do

Access control, quotas, configuration, consumer group administration, delegation tokens, and log
directories are absent. The topic operations above are the ones every backend supports with the
same meaning.
