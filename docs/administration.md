# Administration

`KafkaClient[F].admin` is a `Resource` over the cluster and topic operations,
record deletion, topic configuration, and consumer groups. It neither produces nor
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
- `describeCluster` reports the cluster's id, its brokers, and its controller.
- `describeTopics` reports each topic's id, its partitions, and where their replicas are. See [Cluster and topics](#cluster-and-topics).
- `deleteRecords` deletes each partition's records below an offset. See [Deleting records](#deleting-records).
- `describeTopicConfigurations` reports each topic's effective configuration, and
  `alterTopicConfigurations` changes it. See [Topic configuration](#topic-configuration).
- `electPreferredLeaders` makes each partition's preferred replica its leader. See [Leader election](#leader-election).
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
Creating, growing, and deleting topics all propagate through the cluster in their
own time, so a description taken immediately afterwards may not yet reflect the
request before it.
@:@

## Cluster and topics

`describeCluster` answers the cluster's id, each of its brokers with the host
and port clients connect to, and which broker is its controller.

`describeTopics` answers each requested topic on its own with a
`TopicDescription`: the topic's id, whether it is `internal`, and each
partition's leader, replicas, and in-sync replicas, by `BrokerId`. Look a broker
id up in `describeCluster`'s brokers for its host and port.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("events").liftTo[IO]
    answers <- KafkaClient[IO].admin(client).use(_.describeTopics(NonEmptySet.one(topic)))
    _       <- answers(topic) match
                 case Some(Right(description)) =>
                   IO.println(s"${description.partitions.length} partitions, id ${description.id.value}")
                 case Some(Left(failure)) => IO.println(s"not described: ${failure.getMessage}")
                 case None                => IO.unit
  yield ()
```

- A topic the cluster does not have answers with
  `ErrorCode.UnknownTopicOrPartition`, and the other topics are still answered.
- `internal` is true for the topics Kafka keeps for itself, such as
  `__consumer_offsets`. The cluster creates that one for the first committed
  offset, so a cluster where nothing has committed yet does not have it.
- A topic keeps its id. A topic deleted and created again under the same name
  gets a new one. The id reads the same on every platform.
- Either call fails as a whole when the cluster cannot be reached in time. That
  failure is retriable.

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
    _         <- answers.toSortedMap.toList.traverse_ :
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
    policy   = answers(topic).flatMap(_.toOption).flatMap(_.get("cleanup.policy"))
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

### Changing it

`alterTopicConfigurations` changes the properties it names on each topic and
leaves every other property as it is. Each change is one of:

- `ConfigurationChange.Set(value)` gives the topic its own value.
- `ConfigurationChange.Unset` drops the topic's own value, so the topic takes
  the value it would have without one, such as Kafka's default.
- `ConfigurationChange.Append(value)` and `ConfigurationChange.Subtract(value)`
  add a value to, or remove one from, a property that holds a list, such as
  `cleanup.policy`.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("journal").liftTo[IO]
    changes  = NonEmptyMap.of("retention.ms" -> ConfigurationChange.Set("604800000"), "cleanup.policy" -> ConfigurationChange.Append("compact"))
    answers <- KafkaClient[IO].admin(client).use(_.alterTopicConfigurations(NonEmptyMap.one(topic, changes)))
    _       <- IO.println(answers(topic).fold("no answer")(_.fold(failure => s"not changed: ${failure.getMessage}", _ => "changed")))
  yield ()
```

Each topic answers on its own, and the call fails as a whole only when no topic
got an answer, such as when the cluster cannot be reached in time. Where the
broker refuses one topic's changes, the other topics are still changed:

- A topic the cluster does not have answers with `ErrorCode.UnknownTopicOrPartition`.
- A property Kafka does not have, and appending to or removing from a property
  that holds no list, answer with `ErrorCode.InvalidConfiguration`.

## Leader election

Each partition has a preferred replica, the first of the replicas
`describeTopics` lists for it. Another replica can lead the partition, and
`electPreferredLeaders` makes the preferred one its leader again.

```scala mdoc:compile-only
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client    <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic     <- Topic.from("events").liftTo[IO]
    partition <- Partition.from(0).liftTo[IO]
    answers   <- KafkaClient[IO].admin(client).use(_.electPreferredLeaders(NonEmptySet.one(TopicPartition(topic, partition))))
  yield answers
```

- A partition its preferred replica already leads answers `Right(())`, as one
  the election moved does.
- A partition the cluster does not have answers with
  `ErrorCode.UnknownTopicOrPartition`, and the other partitions are still
  answered.
- The call fails as a whole only when no partition got an answer, such as when
  the cluster cannot be reached in time.

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
