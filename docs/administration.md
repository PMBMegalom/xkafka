# Administration

`KafkaClient[F].admin` is a `Resource` over the topic operations, record
deletion, topic configuration, and consumer groups. It neither produces nor
consumes.

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
- `listConsumerGroups`, `describeConsumerGroups`, `listConsumerGroupOffsets`,
  `alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`, and
  `deleteConsumerGroups` inspect and change consumer groups. See [Consumer groups](#consumer-groups).

`NewTopic.from` rejects a partition count or replication factor that is not
positive. `createPartitions` likewise rejects a non-positive count before
contacting Kafka. The broker still validates cluster-dependent constraints such
as duplicate topics, replication capacity, topic configuration, and whether a
new total partition count is greater than the current one.

@:callout(warning)
Creating, deleting, and growing topics report only the first failure, even
when several topics fail. A request covering several topics may already have
changed the cluster for the others.
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
- The call fails as a whole only when no partition got an answer, such as when
  the cluster cannot be reached in time. That failure is retriable.
- Deleting records moves no consumer group's offsets. A group whose committed
  offset is now below the start of the partition resumes as its
  `autoOffsetReset` says. See [Where a consumer starts](consuming.md#where-a-consumer-starts).

@:callout(warning)
A cut can reach any record written to the partition, including records of a
transaction that has not committed yet. Consumers reading committed records never
see the records such a cut deletes, even after their transaction commits.
@:@

## Topic configuration

`describeTopicConfigurations` reports the configuration each topic has in
effect, including the values it inherits from the broker or from Kafka's
defaults. Each topic answers on its own, as with `deleteRecords`: a topic the
cluster does not have answers with `ErrorCode.UnknownTopicOrPartition`, and the
call fails as a whole only when no topic got an answer, such as when the cluster
cannot be reached in time.

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

## Consumer groups

`listConsumerGroups` answers every group the cluster knows with its
`ConsumerGroupState`, including a group that has no members but still holds
committed offsets. `describeConsumerGroups` answers each requested group with
its state, its members, and the partitions each member is assigned.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group   <- ConsumerGroup.from("billing").liftTo[IO]
    answers <- KafkaClient[IO].admin(client).use(_.describeConsumerGroups(NonEmptySet.one(group)))
    _       <- answers(group) match
                 case Some(Right(description)) =>
                   IO.println(s"${description.state}: ${description.members.map(_.clientId).mkString(", ")}")
                 case Some(Left(failure)) => IO.println(s"not described: ${failure.getMessage}")
                 case None                => IO.unit
  yield ()
```

- A group the cluster does not have answers with `ErrorCode.GroupIdNotFound`,
  and is not listed.
- Kafka keeps committed offsets under an empty group name and lists that
  group, but `listConsumerGroups` leaves it out, because `ConsumerGroup` does
  not accept an empty name.
- A group whose members have all left is `Empty`, with no members and no
  assignor.
- `simple` is true for a group that holds committed offsets but that no
  consumer has joined, such as one created by `alterConsumerGroupOffsets`. It
  is false for a group consumers have joined, including after they all leave.

`listConsumerGroupOffsets` answers the offsets a group has committed. Without
partitions it answers every one, and is empty for a group the cluster does not
have. With partitions it answers each of them, with `None` where the group has
committed nothing.

`alterConsumerGroupOffsets` and `deleteConsumerGroupOffsets` answer each
partition on its own, and `deleteConsumerGroups` each group on its own:

- A group with members refuses `alterConsumerGroupOffsets` as
  `ErrorCode.UnknownMemberId`, and `deleteConsumerGroups` as
  `ErrorCode.NonEmptyGroup`. Stop the group's consumers first.
- `deleteConsumerGroupOffsets` refuses a partition whose topic the group is
  subscribed to as `ErrorCode.GroupSubscribedToTopic`.
- `alterConsumerGroupOffsets` creates a group the cluster does not have. It
  refuses a topic the cluster does not have as
  `ErrorCode.UnknownTopicOrPartition`, and still answers the other partitions.
- `deleteConsumerGroupOffsets` and `deleteConsumerGroups` refuse a group the
  cluster does not have as `ErrorCode.GroupIdNotFound`.
- A call fails as a whole only when no partition or group got an answer, such
  as when the cluster cannot be reached in time. That failure is retriable.

## What it does not do

Access control, quotas, changing configuration, delegation tokens, and log
directories are not covered.
