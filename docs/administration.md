# Administration

`KafkaClient[F].admin` is a `Resource` over the topic operations, record
deletion, and topic configuration. It neither produces nor consumes.

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
- `createPartitions` requests the topic's new total partition count, which Kafka allows only as an increase.
- `describeTopics` reports the partitions of each requested topic.
- `deleteRecords` deletes each partition's records below an offset. See [Deleting records](#deleting-records).
- `describeTopicConfigurations` reports each topic's effective configuration. See [Topic configuration](#topic-configuration).

`NewTopic.from` rejects a partition count or replication factor that is not
positive. `createPartitions` likewise rejects a non-positive count before
contacting Kafka. The broker still validates cluster-dependent constraints such
as duplicate topics, replication capacity, topic configuration, and whether a
new total partition count is greater than the current one.

@:callout(warning)
Kafka reports an outcome for each topic, but the backends do not agree on whether
that detail survives. The topic operations therefore report only the first failure. A
request covering several topics may already have changed the cluster before the
failure it reports.
@:@

@:callout(info)
`describeTopics` fails if the cluster does not have one of the requested topics.
Creating, growing, and deleting topics all propagate through the cluster in their
own time, so a description taken immediately afterwards may not yet reflect the
request before it.
@:@

## Deleting records

`deleteRecords` takes an offset for each partition and deletes the records below
it, so a cut at 3 removes offsets 0 to 2 and keeps 3 onwards. Each partition
answers on its own: with its new low watermark, the offset the partition now
begins at, or with the failure that refused the cut.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.IO
import cats.syntax.all.*

import xkafka.*

val program =
  for
    client    <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic     <- Topic.from("journal").liftTo[IO]
    partition <- Partition.from(0).liftTo[IO]
    cut       <- Offset.from(1000L).liftTo[IO]
    answers   <- KafkaClient[IO].admin(client).use(_.deleteRecords(NonEmptyMap.one(TopicPartition(topic, partition), cut)))
    _         <- answers.toList.traverse_ :
                   case (topicPartition, Right(lowWatermark)) => IO.println(s"${topicPartition.show} begins at ${lowWatermark.show}")
                   case (topicPartition, Left(failure))       => IO.println(s"${topicPartition.show} was refused: ${failure.getMessage}")
  yield ()
```

- A cut at or below the partition's current start deletes nothing and answers
  with the current low watermark, so repeating a cut is safe.
- A cut at the end of the partition deletes all of its records. A cut past the end
  is refused as `ErrorCode.OffsetOutOfRange`, and the other partitions in the same
  call are still answered.
- A topic or partition the cluster does not have answers with
  `ErrorCode.UnknownTopicOrPartition`, and the other partitions in the call are
  still answered.
- The call itself fails only where nothing is known about any partition, such as
  when the cluster cannot be reached in time. It then fails with that failure,
  which is retriable.
- Deleting records moves no consumer group's offsets. A group whose committed
  offset is now below the start of the partition resumes as its
  `autoOffsetReset` says. See [Where a consumer starts](consuming.md#where-a-consumer-starts).

@:callout(warning)
A cut can reach any record written to the partition, including records of a
transaction that has not committed yet. Consumers reading committed records never
see the records such a cut deletes, even after their transaction commits.
@:@

## Topic configuration

@:callout(warning)
`describeTopicConfigurations` is not available on Scala.js. It raises
`KafkaException.Unsupported` there, because `@confluentinc/kafka-javascript`
does not offer the operation. It works on the JVM and Scala Native.
@:@

`describeTopicConfigurations` reports the configuration each topic has in
effect, including the values it inherits from the broker or from Kafka's
defaults. Each topic answers on its own, as with `deleteRecords`: a topic the
cluster does not have answers with `ErrorCode.UnknownTopicOrPartition`, and the
call itself fails only where nothing is known about any topic, such as when the
cluster cannot be reached in time.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("journal").liftTo[IO]
    answers <- KafkaClient[IO].admin(client).use(_.describeTopicConfigurations(NonEmptySet.one(topic)))
    policy   = answers.get(topic).flatMap(_.toOption).flatMap(_.get("cleanup.policy"))
    _       <- IO.println(policy.fold("no cleanup.policy reported")(entry => s"${entry.value} from ${entry.source}"))
  yield ()
```

Each `ConfigurationEntry` carries:

- `value`: `Present` with the value, `Absent` where the property has none, or
  `Redacted` where the value is sensitive and the broker withheld it.
- `source`: where the value comes from. `TopicOverride` is set on the topic
  itself; `DynamicBroker`, `DynamicClusterDefault`, and `StaticBroker` are
  inherited from broker configuration; `Default` is Kafka's own default.
- `readOnly`: whether the property can be changed.

A property the broker did not report has no entry, so `get` answers `None`.

## What it does not do

Access control, quotas, changing configuration, consumer group administration,
delegation tokens, and log directories are not covered.
