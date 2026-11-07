# Running a program

How a running application processes records, what it guarantees, and how to
watch it.

## Guarantees

With `ProcessingGuarantee.AtLeastOnce`, the default, a member commits its
progress every 30 seconds. If its work moves to another member, the records read
since the last commit are processed and written again.

With `ProcessingGuarantee.ExactlyOnce`, a member writes its output and commits
its progress together in one transaction, every 100ms. Each record then counts
exactly once in the application's stores and output topics, even when work
moves between members. Input is read with `IsolationLevel.ReadCommitted`.

These are the same intervals Java Kafka Streams uses. Change them with
`commitInterval`.

@:callout(warning)
Under either guarantee, an effectful step can run more than once for the same
record, such as after a member stops without committing. Exactly once applies to
what the application writes to Kafka, not to what its effects do elsewhere.
@:@

## Tasks

A program's work is divided into tasks. Each
[sub-topology](README.md#describing-a-program) has one task for each partition
number of the topics it reads, and each member is given some of the tasks. A task
keeps its own part of the program's stores. Tasks move between members as members
start and stop.

## Concurrency

By default a member processes one task at a time, like a single Java Kafka
Streams thread. Set `concurrency` higher to process up to that many tasks at
once. Records within each task stay in order. This helps when an effectful step
waits on something outside Kafka.

## State

`state` streams the application's current phase, and then each change to it:

| phase | meaning |
| --- | --- |
| `Joining` | waiting to be given work |
| `Restoring` | refilling its stores from their changelogs before processing |
| `Running` | processing |
| `Lost` | its work was taken away, and it is joining again |

Each state also lists the `problems` the broker reports, such as a missing
input topic or the application not being allowed to use its topics
(`StreamsProblem.TopicAuthorizationFailed`). Like Java Kafka Streams, a member
keeps going for as long as it can. To stop on a problem, watch for it:

```scala mdoc:compile-only
import cats.effect.IO

import xkafka.streams.*

def runUntilRefused(streams: KafkaStreams[IO]): IO[Unit] =
  val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.TopicAuthorizationFailed])).compile.drain
  IO.race(streams.run, refused).void
```

Some brokers are configured to ask applications for a description of their
topology. A member sends the program's description when asked, as Java Kafka
Streams does, and the broker keeps it for whoever describes the group, such as
Java's `Admin.describeStreamsGroups` with `includeTopologyDescription`.
`StreamsSettings`' `withoutTopologyDescriptionPush` stops members sending it:
the application keeps working, and the group's description has none.

## Stores and standbys

Stores are kept in memory. A member given new work first refills its stores from
their changelogs.

If the broker's `streams.num.standby.replicas` setting asks for standby
replicas, members also keep up-to-date copies of other members' stores. When
work moves to a member holding a copy, it only needs to catch up on what the
copy has not yet read.

@:callout(info)
A copy that is no longer needed is kept for 10 minutes, like Java Kafka
Streams' state directories, in case the work comes back.
@:@
