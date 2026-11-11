# Running a program

How a running application processes records, what it guarantees, and how to
watch it.

## Guarantees

With `ProcessingGuarantee.AtLeastOnce`, the default, a member commits its
progress every 30 seconds. If its work moves to another member, the records read
since the last commit are processed and written again.

With `ProcessingGuarantee.ExactlyOnce`, set with `withGuarantee`, a member writes its output and commits
its progress together in one transaction, every 100ms. Each record then counts
exactly once in the application's stores and output topics, even when work
moves between members. Input is read with `IsolationLevel.ReadCommitted`.

These are the same intervals Java Kafka Streams uses. Change them with
`withCommitInterval`.

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
Streams thread. `withConcurrency(n)` processes up to `n` tasks at once. Records within each task stay in order. This helps when an effectful step
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
// Run the application, and stop it if the broker says it may not use its topics
import cats.effect.IO

import xkafka.streams.*

def runUntilRefused(streams: KafkaStreams[IO]): IO[Unit] =
  // Ends once such a problem is reported
  val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.TopicAuthorizationFailed])).compile.drain
  IO.race(streams.run, refused).void  // Whichever ends first stops the other
```

Some brokers are configured to ask applications for a description of their
topology. A member sends the program's description when asked, as Java Kafka
Streams does, and the broker keeps it for whoever describes the group, such as
Java's `Admin.describeStreamsGroups` with `includeTopologyDescription`.
`StreamsSettings`' `withoutTopologyDescriptionPush` stops members sending it:
the application keeps working, and the group's description has none.

## Stores and standbys

By default, stores are kept in memory, and a member refills them from their
changelogs each time it starts or is given new work.

### Keeping stores on disk

Give a state directory to keep stores on disk instead:

```scala mdoc:compile-only
// Keep the application's stores on disk, so a restarted member does not refill them from the start
import cats.data.ValidatedNel

import xkafka.*
import xkafka.streams.*

def onDisk(client: ClientSettings, id: ApplicationId): ValidatedNel[StreamsSettingsError, StreamsSettings] =
  StreamsSettings.from(client, id).andThen(_.withStateDirectory("/var/lib/orders"))  // Stores go under /var/lib/orders/<application id>
```

A member started again over the same directory usually gets back the work it
had, and reads only the changelog records written since it stopped. The same
goes for work that moves to another member and comes back. Give each running
member a directory of its own: a second member started over a directory already
in use fails to start.

Stores on disk are kept by RocksDB 10.1.3. What each platform needs:

- **JVM:** add RocksDB to the application's dependencies. Without it, an
  application given a state directory fails to start, saying what to add.
  ```scala
  // In build.sbt
  libraryDependencies += "org.rocksdb" % "rocksdbjni" % "10.1.3"
  ```
- **Scala.js:** nothing; RocksDB comes from npm with xkafka.
- **Scala Native:** the RocksDB library file where the application runs, as
  [Platform notes](../platform-notes.md#stores-on-disk) describe.

### Keeping a store in memory

A small store that is quick to refill, or one you would rather not write to disk,
can stay in memory while the others go to disk. Name it with `inMemory`:

```scala mdoc:compile-only
// Count each user's orders in a store on disk, and each user's page views in a store kept in memory
import cats.effect.IO

import xkafka.*
import xkafka.streams.*

def counts(orders: Topic, views: Topic, orderCounts: Topic, viewCounts: Topic): List[Pipeline[IO]] =
  val text = Serde.utf8[IO]
  List(
    KStream(Named("read-orders"), orders, text, text).groupByKey(text, text)
      .count(Named("count-orders"), Store("orders"))                  // On disk, where a state directory is given
      .toStream(Named("orders-changes")).to(Named("write-orders"), orderCounts, text, Serde.long[IO]),
    KStream(Named("read-views"), views, text, text).groupByKey(text, text)
      .count(Named("count-views"), Store("views").inMemory)           // In memory, refilled from its changelog on each start
      .toStream(Named("views-changes")).to(Named("write-views"), viewCounts, text, Serde.long[IO])
  )
```

The buffers that [holding back updates](holding-back-updates.md) uses are always
kept in memory.

### Standby copies

If the broker's `streams.num.standby.replicas` setting asks for standby
replicas, members also keep up-to-date copies of other members' stores. When
work moves to a member holding a copy, it only needs to catch up on what the
copy has not yet read.

@:callout(info)
A copy that is no longer needed is kept for 10 minutes, in case the work comes
back. So are the stores on disk of work that moved to another member.
@:@
