# Streams

`KafkaClient.streams` runs a Kafka Streams program as one member of its
application's group. The other members of the application may run xkafka or Java
Kafka Streams: they declare the same topology, read and write the same topics,
and hand tasks to each other as members join and leave.

An application joins a streams group, which needs brokers of Kafka 4.2 or newer.

```scala mdoc:compile-only
import cats.data.NonEmptyList
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val text = Serde.utf8[IO]

val program =
  for
    input    <- Topic.from("words").liftTo[IO]
    output   <- Topic.from("word-counts").liftTo[IO]
    pipeline  = KStream(input, text, text).groupByKey(text, text).count(Store("counts")).toStream.to(output, text, Serde.long[IO])
    topology <- Topology.of(pipeline).liftTo[IO]
    client   <- ClientSettings.from(NonEmptyList.one("localhost:9092")).liftTo[IO]
    id       <- ApplicationId.from("word-counter").liftTo[IO]
    settings <- StreamsSettings.from(client, id).liftTo[IO]
    _        <- KafkaClient[IO].streams(settings, topology).use(_.run)
  yield ()
```

Acquiring the resource joins the group and starts processing. `run` waits while
the application runs, and fails with the error that stops it. Releasing the
resource gives back the member's tasks, committing them, and leaves the group,
waiting at most `closeTimeout`.

## Programs

A program is a `Topology` of pipelines, each from an input topic to an output
topic. A pipeline is built from a `KStream`:

- `filter`, `map`, `mapValues`, `flatMap`, `selectKey` and `peek`, and
  `evalFilter`, `evalMap` and `evalMapValues` for steps that run an effect;
- `groupByKey` and `groupBy`, then `count`, `reduce` or `aggregate` into a named
  `Store`, giving a `KTable` whose `toStream` is each change to it;
- `to`, which ends the pipeline at its output topic.

Records whose key may have changed before they are grouped, after `map`,
`flatMap`, `selectKey` or `evalMap`, go through a repartition topic. `groupBy`
names it with a `Repartition`, and so does `groupByKey` after such a step. A store keeps its contents in a
changelog topic. The application's own topics are named after it:
`<application id>-<store>-changelog` and `<application id>-<name>-repartition`.
The broker creates them as the application first joins.

`Topology.of` checks the names and reports every problem at once, such as two
stores with one name or two pipelines reading one topic. `liftTo` raises the
problems in `F` as one `InvalidTopology`, and refused settings as one
`InvalidStreamsSettings`.

A `Serde` pairs the serializer and deserializer of one type, since a streams
application both reads and writes its topics. `Serde.utf8`, `Serde.long` and
`Serde.bytes` write what Java Kafka Streams' `Serdes.String()`, `Serdes.Long()`
and `Serdes.ByteArray()` write.

## Guarantees

`ProcessingGuarantee.AtLeastOnce`, the default, commits what each task read
after writing what it wrote, every 30 seconds. A task that moves to another
member reads again what it read since its last commit, and writes it again.

`ProcessingGuarantee.ExactlyOnce` writes what the tasks wrote and commits what
they read in one transaction, every 100ms. A task that moves to another member
finds what it wrote only together with where it read to, so each record counts
once in the application's stores and outputs. It reads its input with
`IsolationLevel.ReadCommitted`.

These are the intervals Java Kafka Streams uses for each guarantee. Change them
with `commitInterval`.

@:callout(warning)
Under either guarantee, an effectful step runs again for a record read again,
such as after a member stops without committing. Exactly once covers what the
application writes to Kafka, not what its effects do elsewhere.
@:@

## Concurrency

A member processes one task at a time by default, as a single Java Kafka Streams
thread does. With `concurrency` set above one, up to that many tasks process at
once, each its own records in order. This helps where an effectful step waits on
something outside Kafka.

## State

`state` gives the application's phase, first as it is and then each time it
changes, with what the group coordinator reports wrong with the application:

| phase | meaning |
| --- | --- |
| `Joining` | waiting for the group to give it tasks |
| `Restoring` | refilling the stores of the tasks it was given from their changelogs |
| `Running` | processing its tasks |
| `Lost` | its tasks were taken away without being given back, and it is joining again |

`problems` lists what the group coordinator reports, such as missing source
topics, internal topics being created, or the application being refused its
topics (`StreamsProblem.TopicAuthorizationFailed`). The member goes on as long as
it can, as Java Kafka Streams does, so an application that should stop on a
problem watches for it:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.streams.*

def runUntilRefused(streams: KafkaStreams[IO]): IO[Unit] =
  val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.TopicAuthorizationFailed])).compile.drain
  IO.race(streams.run, refused).void
```

A broker configured with a topology description plugin asks the members of a
streams group for a description of their topology. xkafka members don't send
one, and keep their tasks without it.

## Stores and standbys

Stores are held in memory. A member given a task refills the task's stores from
their changelogs before processing it.

When the group keeps standby replicas, which the broker's
`streams.num.standby.replicas` group setting asks for, a member also keeps
standby copies of other members' stores, following their changelogs. When such a
task becomes the member's own, it restores only what its copy had not yet read.

@:callout(info)
A copy the group takes away is kept for 10 minutes, as Java Kafka Streams keeps a
task's state directory, in case the task comes back.
@:@
