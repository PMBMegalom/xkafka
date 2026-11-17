# Administration

`KafkaClient[F].admin(client)` gives you a `Resource` for managing topics, their
records and configuration, and consumer groups.

```scala mdoc:compile-only
// Create the topic "events" with six partitions, each on three brokers
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic    <- Topic.from("events").liftTo[IO]
    newTopic <- NewTopic.from(topic, partitions = 6, replicationFactor = 3).liftTo[IO]  // Rejects counts that are not positive
    // Acquire an admin client, create the topic, and release the client
    _        <- KafkaClient[IO].admin(client).use(_.createTopics(NonEmptySet.one(newTopic)))
  yield ()
```

## What it does

- `createTopics` creates each topic with the partitions, replication factor, and
  configuration given, and fails if one already exists.
- `deleteTopics` deletes each topic, and fails if one does not exist.
- `createPartitions` grows a topic to a new total partition count, and fails
  if the topic already has that many or more.
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
contacting Kafka. The broker checks the rest: that the topic does not already
exist, that there are enough brokers for the replication factor, that the
configuration is valid, and that a new partition count is larger than the
current one.

@:callout(warning)
Creating, deleting, and growing topics report only the first failure, even
when several topics fail. A request covering several topics may already have
changed the cluster for the others.
@:@

@:callout(info)
Creating, growing and deleting topics take a moment to reach every broker, so
describing a topic right afterwards can still show the old state.
@:@

## Cluster and topics

`describeCluster` returns the cluster's id, each of its brokers with the host
and port clients connect to, and which broker is its controller.

`describeTopics` returns a result for each requested topic on its own, a
`TopicDescription`: the topic's id, whether it is `internal`, and each
partition's leader, replicas, and in-sync replicas, by `BrokerId`. Look a broker
id up in `describeCluster`'s brokers for its host and port.

```scala mdoc:compile-only
// Describe the topic "events", and print its partition count and id
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("events").liftTo[IO]
    results <- KafkaClient[IO].admin(client).use(_.describeTopics(NonEmptySet.one(topic)))  // The result for each topic is separate
    _       <- results(topic) match
                 case Some(Right(description)) =>  // The topic's description
                   IO.println(s"${description.partitions.length} partitions, id ${description.id.value}")
                 // Why this topic could not be described
                 case Some(Left(failure)) => IO.println(s"not described: ${failure.getMessage}")
                 case None                => IO.unit
  yield ()
```

- The result for a topic the cluster does not have is
  `ErrorCode.UnknownTopicOrPartition`, and the other topics still get theirs.
- `internal` is true for the topics Kafka keeps for itself, such as
  `__consumer_offsets`.
- A topic keeps its id. A topic deleted and created again under the same name
  gets a new one. The id reads the same on every platform.
- Either call fails as a whole when the cluster cannot be reached in time. That
  failure is retriable.

## Deleting records

`deleteRecords` takes an offset for each partition and deletes the records below
it, so a cut at 3 removes offsets 0 to 2 and keeps 3 onwards. Each partition
gets a result of its own: its new low watermark, the offset the partition now
begins at, or the failure that refused the cut.

```scala mdoc:compile-only
// Delete the records below offset 1000 in partition 0 of "journal"
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
    // Delete the records, with a separate result for each partition
    results   <- KafkaClient[IO].admin(client).use(_.deleteRecords(NonEmptyMap.one(TopicPartition(topic, partition), cut)))
    _         <- results.toSortedMap.toList.traverse_ :
                   // The offset this partition now begins at
                   case (topicPartition, Right(lowWatermark)) => IO.println(s"${topicPartition.show} begins at ${lowWatermark.show}")
                   // Why the records of this partition were not deleted
                   case (topicPartition, Left(failure))       => IO.println(s"${topicPartition.show} was refused: ${failure.getMessage}")
  yield ()
```

- A cut at or below the partition's current start deletes nothing and returns
  the current low watermark, so repeating a cut is safe.
- A cut at the end of the partition deletes all of its records. A cut past the end
  is refused as `ErrorCode.OffsetOutOfRange`, and the other partitions in the same
  call still get their results.
- The result for a topic or partition the cluster does not have is
  `ErrorCode.UnknownTopicOrPartition`, and the other partitions in the call
  still get theirs.
- The call fails as a whole only if no partition got a result, for example if
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
defaults. Each topic gets a result of its own, as with `deleteRecords`: the
result for a topic the cluster does not have is
`ErrorCode.UnknownTopicOrPartition`, and the call fails as a whole only if no
topic got a result, for example if the cluster cannot be reached in time.

```scala mdoc:compile-only
// Print the value of "cleanup.policy" on "journal", and where that value comes from
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("journal").liftTo[IO]
    // Read the topic's effective configuration
    results <- KafkaClient[IO].admin(client).use(_.describeTopicConfigurations(NonEmptySet.one(topic)))
    policy   = results(topic).flatMap(_.toOption).flatMap(_.get("cleanup.policy"))  // Pick one property from the topic's result
    _       <- IO.println(policy.fold("no cleanup.policy reported")(entry => s"${entry.value} from ${entry.source}"))
  yield ()
```

Each `ConfigurationEntry` carries:

- `value`: `Present` with the value, `Absent` if the property has none, or
  `Redacted` if the value is sensitive and the broker withheld it.
- `source`: where the value comes from. `TopicOverride` is set on the topic
  itself; `DynamicBroker`, `DynamicClusterDefault`, and `StaticBroker` are
  inherited from broker configuration; `Default` is Kafka's own default.
- `readOnly`: whether the property can be changed.

A property the broker did not report has no entry, so `get` returns `None`.

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
// Set the retention of "journal" to seven days, and add "compact" to its cleanup policy
import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic   <- Topic.from("journal").liftTo[IO]
    // Set one property, and append to a list property
    changes  = NonEmptyMap.of("retention.ms" -> ConfigurationChange.Set("604800000"), "cleanup.policy" -> ConfigurationChange.Append("compact"))
    // Every other property stays as it is
    results <- KafkaClient[IO].admin(client).use(_.alterTopicConfigurations(NonEmptyMap.one(topic, changes)))
    _       <- IO.println(results(topic).fold("no result")(_.fold(failure => s"not changed: ${failure.getMessage}", _ => "changed")))
  yield ()
```

Each topic gets a result of its own, and the call fails as a whole only if no
topic got a result, for example if the cluster cannot be reached in time. If the
broker refuses one topic's changes, the other topics are still changed:

- The result for a topic the cluster does not have is
  `ErrorCode.UnknownTopicOrPartition`.
- A property Kafka does not have, and appending to or removing from a property
  that holds no list, fail with `ErrorCode.InvalidConfiguration`.

## Leader election

Each partition has a preferred replica, the first of the replicas
`describeTopics` lists for it. Another replica can lead the partition, and
`electPreferredLeaders` makes the preferred one its leader again.

```scala mdoc:compile-only
// Make the preferred replica the leader of partition 0 of "events"
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client    <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    topic     <- Topic.from("events").liftTo[IO]
    partition <- Partition.from(0).liftTo[IO]
    // Run the election for this partition
    results   <- KafkaClient[IO].admin(client).use(_.electPreferredLeaders(NonEmptySet.one(TopicPartition(topic, partition))))
  yield results
```

- The result for a partition its preferred replica already leads is `Right(())`,
  as for one the election moved.
- The result for a partition the cluster does not have is
  `ErrorCode.UnknownTopicOrPartition`, and the other partitions still get
  theirs.
- The call fails as a whole only if no partition got a result, for example if
  the cluster cannot be reached in time.

## Consumer groups

`listConsumerGroups` returns every group the cluster knows, with its
`ConsumerGroupState`, including a group that has no members but still holds
committed offsets. `describeConsumerGroups` returns each requested group with
its state, its members, and the partitions each member is assigned.

```scala mdoc:compile-only
// Print the state and members of the consumer group "billing"
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import xkafka.*

val program =
  for
    client  <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    group   <- ConsumerGroup.from("billing").liftTo[IO]
    // The result for each group is separate
    results <- KafkaClient[IO].admin(client).use(_.describeConsumerGroups(NonEmptySet.one(group)))
    _       <- results(group) match
                 case Some(Right(description)) =>
                   IO.println(s"${description.state}: ${description.members.map(_.clientId).mkString(", ")}")
                 case Some(Left(failure)) => IO.println(s"not described: ${failure.getMessage}")
                 case None                => IO.unit
  yield ()
```

- The result for a group the cluster does not have is
  `ErrorCode.GroupIdNotFound`, and the group is not listed.
- `listConsumerGroups` leaves out a group with an empty name, which
  `ConsumerGroup` cannot hold.
- A group whose members have all left is `Empty`, with no members and no
  assignor.
- `simple` is true for a group that holds committed offsets but that no
  consumer has joined, such as one created by `alterConsumerGroupOffsets`. It
  is false for a group consumers have joined, including after they all leave.

`listConsumerGroupOffsets` returns the offsets a group has committed. Without
partitions it returns every one, and is empty for a group the cluster does not
have. With partitions it returns each of them, with `None` if the group has
committed nothing.

`alterConsumerGroupOffsets` and `deleteConsumerGroupOffsets` return a result for
each partition on its own, and `deleteConsumerGroups` for each group on its own:

- A group with members refuses `alterConsumerGroupOffsets` as
  `ErrorCode.UnknownMemberId`, and `deleteConsumerGroups` as
  `ErrorCode.NonEmptyGroup`. Stop the group's consumers first.
- `deleteConsumerGroupOffsets` refuses a partition whose topic the group is
  subscribed to as `ErrorCode.GroupSubscribedToTopic`.
- `alterConsumerGroupOffsets` creates a group the cluster does not have. It
  refuses a topic the cluster does not have as
  `ErrorCode.UnknownTopicOrPartition`, and the other partitions still get their
  results.
- `deleteConsumerGroupOffsets` and `deleteConsumerGroups` refuse a group the
  cluster does not have as `ErrorCode.GroupIdNotFound`.
- A call fails as a whole only if no partition or group got a result, for
  example if the cluster cannot be reached in time. That failure is retriable.

## What it does not do

Access control, quotas, changing broker configuration, delegation tokens, and log
directories are not covered.
