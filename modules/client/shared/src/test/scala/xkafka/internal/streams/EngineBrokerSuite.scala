/*
 * Copyright (c) 2026 xkafka contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package xkafka
package internal.streams

import java.nio.ByteBuffer

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import internal.driver.StartAt
// `streams` alone names this package, so the program's own package is named in full.
import xkafka.streams.{
  ApplicationId, Flow, GlobalKTable, JoinWindows, KStream, KTable, Named, Partitioner, Pipeline, ProcessingGuarantee, Repartition, Serde,
  SessionWindows, SlidingWindows, Store, StreamsPhase, StreamsProblem, StreamsSettings, Suppressed, TimeWindows, Topology
}

/** The engine running a program against each broker release `scripts/integration-test.sh` starts with `--streams`, without Java members. */
final class EngineBrokerSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 5.minutes

  private val brokers =
    PlatformKafkaClient.environment("XKAFKA_STREAMS_BROKERS").toList.flatMap(_.split(',').toList).map: entry =>
      val Array(release, server) = entry.split('=')
      (release, server)

  private val text = Serde.utf8[IO]

  /** An application of its own, counting its input's records into a store, and writing each new count to its output. */
  private final case class Counting(application: String, input: Topic, output: Topic, program: (Topic, Topic) => Pipeline[IO]):
    val changelog: Topic       = Topic.from(s"$application-counts-changelog").orFail
    val topology: Topology[IO] = Topology.of(program(input, output)).orFail

  /** Counts by key. */
  private def byKey(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-1"), input, text, text).groupByKey(text, text).count(Named("count-1"), Store("counts")).toStream(Named("to-stream-1"))
      .to(Named("write-1"), output, text, Serde.long[IO])

  /** Counts by the value's first letter, through a repartition topic keyed by it. */
  private def byLetter(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-2"), input, text, text).groupBy(Repartition("by-letter"), text, text)((_, value) => value.take(1))
      .count(Named("count-2"), Store("counts")).toStream(Named("to-stream-2")).to(Named("write-2"), output, text, Serde.long[IO])

  /** Counts by key in tumbling windows of a minute, taking records up to an hour late. */
  private def byMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-3"), input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 1.hour))
      .count(Named("count-3"), Store("counts")).toStream(Named("to-stream-3"))
      .to(Named("write-3"), output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Counts by key in tumbling windows of a minute, taking records up to ten seconds late, kept an hour, writing only each window's final count. */
  private def finalByMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-4"), input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
      .withRetention(1.hour).emitOnWindowClose.count(Named("count-4"), Store("counts")).toStream(Named("to-stream-4"))
      .to(Named("write-4"), output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Counts as `finalByMinute` does, passing on each window's final count once it closes through the suppression `final`. */
  private def suppressedByMinute(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-5"), input, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
      .count(Named("count-5"), Store("counts")).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-5"))
      .to(Named("write-5"), output, Serde.timeWindowed(text, 1.minute), Serde.long[IO])

  /** Reads the input as a table into the store `counts`, writing each change, a delete as a tombstone. */
  private def asTable(input: Topic, output: Topic): Pipeline[IO] =
    KTable(Named("counts"), input, text, text, Store("counts")).toStream(Named("to-stream-6")).to(Named("write-6"), output, text, text.option)

  /** Takes each word of each value but "skip", keyed by its first letter through a repartition asked for by hand, as a table of each letter's latest
    * word, kept in `latest`; then its length, kept in `counts`; then only lengths over one, so a shorter word deletes its letter.
    */
  private def throughFilter(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-8"), input, text, text).filterNot(Named("not-skip"))((_, value) => value == "skip")
      .flatMapValues(Named("each"))(_.split(' ').toList).selectKey(Named("by-letter"))((_, value) => value.take(1))
      .repartition(Repartition("through"), text, text).toTable(Named("latest"), Store("latest"), text, text)
      .mapValues(Named("lengths"), Store("counts"), text)(_.length.toString).filter(Named("long"))((_, length) => length != "1")
      .toStream(Named("to-stream-9")).to(Named("write-9"), output, text, text.option)

  /** Waits up to two minutes until `topic` holds at least `count` records. */
  private def holding(server: String, topic: Topic, count: Int)(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.bytes[IO].option,
          Deserializer.bytes[IO].option,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.take(count.toLong).compile.drain.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $count records")))

  /** The first `count` records of `topic`, in up to two minutes. */
  private def read[V](server: String, topic: Topic, count: Int, value: Serde[IO, V])(using guarantee: ProcessingGuarantee): IO[List[(String, V)]] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          value.deserializer,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.take(count.toLong).map(found => (found.record.key, found.record.value)).compile.toList
          .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $count records")))

  /** Counts by key in sliding windows of five seconds, taking records up to an hour late. */
  private def bySliding(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-6"), input, text, text).groupByKey(text, text).windowedBy(SlidingWindows(5.seconds, grace = 1.hour))
      .count(Named("count-6"), Store("counts")).toStream(Named("to-stream-7"))
      .to(Named("write-7"), output, Serde.timeWindowed(text, 5.seconds), Serde.long[IO])

  /** Counts by key in sessions with an inactivity gap of five seconds, taking records up to an hour late. */
  private def bySession(input: Topic, output: Topic): Pipeline[IO] =
    KStream(Named("read-7"), input, text, text).groupByKey(text, text).windowedBy(SessionWindows(5.seconds, grace = 1.hour))
      .count(Named("count-7"), Store("counts")).toStream(Named("to-stream-8"))
      .to(Named("write-8"), output, Serde.sessionWindowed(text), Serde.long[IO].option)

  /** Reads `topic` from its start until the latest count of each key in each session, by its start and end, or its tombstone, is `expected`. */
  private def sessionCounts(server: String, topic: Topic, expected: Map[(String, Long, Long), Option[Long]])(using
      guarantee: ProcessingGuarantee
  ): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Option[Long]]((_, _, value) => IO(value.map(bytes => ByteBuffer.wrap(bytes.toArray).getLong)))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.sessionWindowed(text).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[(String, Long, Long), Option[Long]]): (seen, record) =>
          val windowed = record.record.key
          seen.updated((windowed.key, windowed.window.start, windowed.window.end), record.record.value)
        .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Writes a record for each key at each timestamp, in order. */
  private def produceAt(server: String, topic: Topic, records: List[(String, Long)]): IO[Unit] =
    val made = records.map((key, at) => ProducerRecord(topic, key, s"$key-value", timestamp = Some(Timestamp.fromEpochMillis(at))))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(made))).void

  /** Where each of `partitions` ends, by partition number, which purging its records does not move. */
  private def endsOf(server: String, partitions: Set[TopicPartition]): IO[Map[Int, Long]] =
    Names.fresh:
      val settings =
        ConsumerSettings
          .from(client(server), ConsumerGroup.from(summon[Names]("xkafka-engine-ends")).orFail, Deserializer.utf8[IO], Deserializer.utf8[IO]).orFail
      PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptyList.fromListUnsafe(partitions.toList).toNes)).use: consumer =>
        consumer.endOffsets(partitions).map(_.map((partition, end) => partition.partition.value -> end.value))

  /** Writes each key's value to the partition given. */
  private def produceTo(server: String, topic: Topic, records: List[(String, String, Int)]): IO[Unit] =
    val made = records.map((key, value, partition) => ProducerRecord(topic, key, value, partition = Some(Partition.from(partition).orFail)))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(made))).void

  /** The first `count` records of `topic`, each with the partition it is in, in up to two minutes. */
  private def placed(server: String, topic: Topic, count: Int)(using guarantee: ProcessingGuarantee): IO[List[(String, String, Int)]] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          Deserializer.utf8[IO],
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.take(count.toLong).map(found => (found.record.key, found.record.value, found.record.topicPartition.partition.value)).compile
          .toList.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $count records")))

  /** Writes each key's value, or a tombstone. */
  private def produceValues(server: String, topic: Topic, records: List[(String, Option[String])]): IO[Unit] =
    val made = records.map((key, value) => ProducerRecord(topic, key, value))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO].option).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(made))).void

  /** Reads `topic` from its start until the latest value of each key, or its tombstone, is `expected`. */
  private def latestValues(server: String, topic: Topic, expected: Map[String, Option[String]])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          Deserializer.utf8[IO].option,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[String, Option[String]])((seen, record) => seen.updated(record.record.key, record.record.value))
          .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Reads `topic` from its start until each key of `expected` has the latest value, or tombstone, that `expected` gives it, whatever other keys
    * hold, failing after two minutes with what it read.
    */
  private def heldValues(server: String, topic: Topic, expected: Map[String, Option[String]])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          Deserializer.utf8[IO].option,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      IO.ref(Map.empty[String, Option[String]]).flatMap: last =>
        PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
          consumer.records.scan(Map.empty[String, Option[String]])((seen, record) => seen.updated(record.record.key, record.record.value))
            .evalTap(last.set).find(seen => expected.forall((key, value) => seen.get(key).contains(value))).compile.lastOrError.void
            .timeoutTo(2.minutes, last.get.flatMap(seen => IO.raiseError(new AssertionError(s"$topic never held $expected; it held $seen"))))

  /** Reads `topic` from its start until the latest count of each key in each window, by the window's start, is `expected`. */
  private def windowCounts(server: String, topic: Topic, expected: Map[(String, Long), Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.timeWindowed(text, 1.minute).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[(String, Long), Long]): (seen, record) =>
          seen.updated((record.record.key.key, record.record.key.window.start), record.record.value)
        .find(_ == expected).compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Reads `topic` from its start until it has held exactly the records `expected` gives, one for each key and window, and no more. */
  private def windowRecords(server: String, topic: Topic, expected: Map[(String, Long), Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Serde.timeWindowed(text, 1.minute).deserializer,
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Vector.empty[((String, Long), Long)])((seen, record) =>
          seen :+ ((record.record.key.key, record.record.key.window.start) -> record.record.value)
        ).find(seen => seen.size == expected.size && seen.toMap == expected).compile.lastOrError
          .timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held exactly $expected"))).void

  private def counting(server: String, program: (Topic, Topic) => Pipeline[IO] = byKey)(test: Counting => IO[Unit]): IO[Unit] =
    Names.fresh:
      val application = summon[Names](s"xkafka-engine-${PlatformKafkaClient.name}")
      val app         = Counting(application, Topic.from(s"$application-input").orFail, Topic.from(s"$application-output").orFail, program)
      val made        = List(app.input, app.output).map(NewTopic.from(_, 2, 1).orFail)
      admin(server).use(_.createTopics(NonEmptySet.of(made.head, made.tail*))) *> test(app)

  private def client(server: String): ClientSettings = ClientSettings.from(NonEmptyList.one(server)).orFail

  private def admin(server: String) = PlatformKafkaClient().admin(client(server))

  /** Writes `count` records for each key, spread over both partitions by key, each valued by `value` from its key and its number. */
  private def produce(server: String, topic: Topic, counts: Map[String, Int], value: (String, Int) => String = (key, _) => s"$key-value"): IO[Unit] =
    val records = counts.toList.flatMap((key, count) => List.tabulate(count)(n => ProducerRecord(topic, key, value(key, n))))
    PlatformKafkaClient().producer(ProducerSettings.from(client(server), Serializer.utf8[IO], Serializer.utf8[IO]).orFail)
      .use(_.produceAndAwait(NonEmptyList.fromListUnsafe(records))).void

  /** Reads `topic` from its start until the latest count of each key is `expected`, failing after two minutes: under read_committed where the engine
    * writes exactly once, so that only what its committed transactions wrote is read.
    */
  private def latest(server: String, topic: Topic, expected: Map[String, Long])(using guarantee: ProcessingGuarantee): IO[Unit] =
    Names.fresh:
      val counts   = Deserializer.instance[IO, Long]((_, _, value) => IO(ByteBuffer.wrap(value.get.toArray).getLong))
      val settings =
        ConsumerSettings.from(
          client(server),
          ConsumerGroup.from(summon[Names]("xkafka-engine-reader")).orFail,
          Deserializer.utf8[IO],
          counts,
          AutoOffsetReset.Earliest,
          isolationLevel = if guarantee == ProcessingGuarantee.ExactlyOnce then IsolationLevel.ReadCommitted else IsolationLevel.ReadUncommitted
        ).orFail
      PlatformKafkaClient().consumer(settings, Selection.Topics(NonEmptySet.one(topic))).use: consumer =>
        consumer.records.scan(Map.empty[String, Long])((seen, record) => seen.updated(record.record.key, record.record.value)).find(_ == expected)
          .compile.lastOrError.timeoutTo(2.minutes, IO.raiseError(new AssertionError(s"$topic never held $expected"))).void

  /** Runs the engine for `app` while `use` runs, failing if the engine stops first, committing every second at least once and at Java's interval
    * exactly once.
    */
  private def running(server: String, app: Counting)(use: IO[Unit])(using guarantee: ProcessingGuarantee): IO[Unit] =
    val interval = Option.when(guarantee == ProcessingGuarantee.AtLeastOnce)(1.second)
    Engine(
      PlatformKafkaClient(),
      KafkaClientPlatform.driver[IO],
      client(server),
      app.application,
      app.topology,
      guarantee,
      interval,
      purgeInterval = 1.second
    ).use(engine => engine.run.race(use).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure)))

  /** Waits up to a minute until each partition of `topic` starts where `group` committed it, past some records: under exactly once, a partition ends
    * with the marker of the transaction that committed it, after the offset committed.
    */
  private def purgedTo(server: String, group: String, topic: Topic): IO[Unit] =
    Names.fresh:
      val partitions = Set(0, 1).map(n => TopicPartition(topic, Partition.from(n).orFail))
      val settings   =
        ConsumerSettings
          .from(client(server), ConsumerGroup.from(summon[Names]("xkafka-engine-watermarks")).orFail, Deserializer.utf8[IO], Deserializer.utf8[IO])
          .orFail
      (admin(server), PlatformKafkaClient().consumer(settings, Selection.Partitions(NonEmptyList.fromListUnsafe(partitions.toList).toNes))).tupled
        .use: (admin, consumer) =>
          def check(left: Int): IO[Unit] =
            (consumer.beginningOffsets(partitions), admin.listConsumerGroupOffsets(ConsumerGroup.from(group).orFail)).tupled.flatMap:
              (beginnings, committed) =>
                val ours = committed.view.filterKeys(partitions).mapValues(_.offset).toMap
                if ours.nonEmpty && ours.forall((partition, offset) => beginnings.get(partition).contains(offset)) && ours.values.exists(_.value > 0)
                then IO.unit
                else if left <= 0 then IO.raiseError(new AssertionError(s"$topic starts at $beginnings, committed at $ours"))
                else IO.sleep(500.millis) *> check(left - 1)
          check(120)

  /** Writes a record for each key in a transaction that aborts. */
  private def aborted(server: String, topic: Topic, keys: List[String]): IO[Unit] =
    Names.fresh:
      val id       = TransactionalId.from(summon[Names]("xkafka-engine-aborted")).orFail
      val settings = TransactionalProducerSettings.from(client(server), id, Serializer.utf8[IO], Serializer.utf8[IO]).orFail
      PlatformKafkaClient().transactionalProducer(settings).use: producer =>
        producer.transactionally: transaction =>
          transaction.produce(NonEmptyList.fromListUnsafe(keys.map(key => ProducerRecord(topic, key, "aborted")))) *>
            IO.raiseError(new RuntimeException("abort"))
        .attempt.void

  /** An engine for `app`, as `running` starts it, whose restores and the changelogs it follows are recorded, keeping its stores under `state`. */
  private def watched(
      server: String,
      app: Counting,
      restores: Ref[IO, List[Map[TopicPartition, Long]]],
      follows: Ref[IO, List[Vector[StartAt]]],
      state: Option[String] = None
  )(using guarantee: ProcessingGuarantee) =
    val interval                                         = Option.when(guarantee == ProcessingGuarantee.AtLeastOnce)(1.second)
    def watch(plane: Engine.Plane[IO]): Engine.Plane[IO] =
      new Engine.Plane[IO]:
        export plane.{ends, followed, poll, purge, read, write}
        def restore(changelogs: Map[TopicPartition, String], from: Map[TopicPartition, Long], stores: Map[String, TaskStore[IO]]) =
          restores.update(_ :+ from) *> plane.restore(changelogs, from, stores)
        def follow(starts: Vector[StartAt]) = follows.update(_ :+ starts) *> plane.follow(starts)
    Engine(
      PlatformKafkaClient(),
      KafkaClientPlatform.driver[IO],
      client(server),
      app.application,
      app.topology,
      guarantee,
      interval,
      stateDirectory = state,
      watch = watch
    )

  /** Waits up to a minute for `condition` to hold. */
  private def eventually(what: String)(condition: IO[Boolean]): IO[Unit] =
    def loop(left: Int): IO[Unit] =
      condition.flatMap: holds =>
        if holds then IO.unit
        else if left <= 0 then IO.raiseError(new AssertionError(s"$what did not happen within a minute"))
        else IO.sleep(250.millis) *> loop(left - 1)
    loop(240)

  private val third = Map("b" -> 1, "d" -> 2)

  private val first  = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
  private val second = Map("a" -> 2, "c" -> 1, "e" -> 5)

  private def total(counts: Map[String, Int]*): Map[String, Long] = counts.toList.combineAll.view.mapValues(_.toLong).toMap

  for
    (release, server)         <- brokers
    given ProcessingGuarantee <- ProcessingGuarantee.values.toList
    under = summon[ProcessingGuarantee]
  do
    test(s"$release, $under: an engine counts each key's records, writing each count to its output and to its store's changelog"):
      counting(server): app =>
        produce(server, app.input, first) *>
          running(server, app)(latest(server, app.output, total(first)) *> latest(server, app.changelog, total(first)))

    test(s"$release, $under: a client runs a streams program, whose state reaches running"):
      counting(server): app =>
        val settings =
          StreamsSettings
            .from(client(server), ApplicationId.from(app.application).orFail, under, Option.when(under == ProcessingGuarantee.AtLeastOnce)(1.second))
            .orFail
        produce(server, app.input, first) *> PlatformKafkaClient().streams(settings, app.topology).use: streams =>
          val running = streams.state.find(_.phase == StreamsPhase.Running).compile.lastOrError.timeout(2.minutes).void
          streams.run.race(running *> latest(server, app.output, total(first))).flatMap:
            case Left(_)  => IO.raiseError(new AssertionError("the application stopped"))
            case Right(_) => IO.unit

    test(s"$release, $under: an engine joins each record with its key's value in a table read from a topic copartitioned with the stream"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-8"), orders, text, text)
            .join(Named("join-users"), KTable(Named("users"), Topic.from(s"${orders.value}-users").orFail, text, text, Store("users")))((o, u) =>
              s"$o:$u"
            ).to(Named("write-9"), output, text, text)
      ): app =>
        val users = Topic.from(s"${app.input.value}-users").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 2, 1).orFail))) *>
          produceValues(server, users, List("a" -> Some("ann"), "b" -> Some("bob"))) *> running(server, app):
            // Written once the users are in the table, so each order finds its user.
            latestValues(server, Topic.from(s"${app.application}-users-changelog").orFail, Map("a" -> Some("ann"), "b" -> Some("bob"))) *>
              produceValues(server, app.input, List("a" -> Some("o1"), "b" -> Some("o2"), "c" -> Some("o3"))) *>
              latestValues(server, app.output, Map("a" -> Some("o1:ann"), "b" -> Some("o2:bob")))

    test(s"$release, $under: an engine outer joins two tables read from copartitioned topics, a change to either updating the result"):
      counting(
        server,
        (plans, output) =>
          KTable(Named("plans"), plans, text, text, Store("plans")).outerJoin(
            Named("outer-join-users"),
            KTable(Named("users"), Topic.from(s"${plans.value}-users").orFail, text, text, Store("users")),
            Store("accounts"),
            text
          )(_.fold(plan => s"$plan:-", user => s"-:$user", (plan, user) => s"$plan:$user")).toStream(Named("to-stream-9"))
            .to(Named("write-10"), output, text, text.option)
      ): app =>
        val users = Topic.from(s"${app.input.value}-users").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 2, 1).orFail))) *> running(server, app):
          produceValues(server, users, List("a" -> Some("ann"), "b" -> Some("bob"))) *>
            latestValues(server, app.output, Map("a" -> Some("-:ann"), "b" -> Some("-:bob"))) *>
            produceValues(server, app.input, List("a" -> Some("gold"), "c" -> Some("free"))) *>
            latestValues(server, app.output, Map("a" -> Some("gold:ann"), "b" -> Some("-:bob"), "c" -> Some("free:-"))) *>
            produceValues(server, users, List("b" -> None)) *>
            latestValues(server, app.output, Map("a" -> Some("gold:ann"), "b" -> None, "c" -> Some("free:-"))) *> latestValues(
              server,
              Topic.from(s"${app.application}-accounts-changelog").orFail,
              Map("a" -> Some("gold:ann"), "b" -> None, "c" -> Some("free:-"))
            )

    test(s"$release, $under: an engine outer joins two streams in windows, sending a record that matched nothing once its window closes"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-9"), orders, text, text).outerJoin(
            Named("pairs"),
            KStream(Named("read-payments"), Topic.from(s"${orders.value}-payments").orFail, text, text),
            JoinWindows.withNoGrace(1.second),
            Store("pairs"),
            text,
            text,
            text
          )(_.fold(order => s"$order:-", payment => s"-:$payment", (order, payment) => s"$order:$payment")).to(Named("write-11"), output, text, text)
      ): app =>
        val payments = Topic.from(s"${app.input.value}-payments").orFail
        // Times five minutes back, as the other tests take them, so that no topic's retention removes the records.
        IO.realTime.flatMap: now =>
          val at = (now.toMillis / 60000L - 5L) * 60000L
          // Java looks for held records whose windows have closed at most once a second by the wall clock, when a record comes at the stream time:
          // records well after the windows, to keys on every partition, each second, until a look sends the held ones on.
          val closing =
            fs2.Stream.iterate(1L)(_ + 1)
              .evalMap(n => produceAt(server, app.input, (0 until 10).toList.map(i => s"z$i" -> (at + 60000L + n * 1000L)))).metered(1.second).compile
              .drain
          admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(payments, 2, 1).orFail))) *> running(server, app):
            produceAt(server, app.input, List("a" -> (at + 1000L), "b" -> (at + 2000L))) *>
              produceAt(server, payments, List("a" -> (at + 1500L), "c" -> (at + 3000L))) *>
              heldValues(server, app.output, Map("a" -> Some("a-value:a-value"))) *> closing.background.surround:
                heldValues(server, app.output, Map("a" -> Some("a-value:a-value"), "b" -> Some("b-value:-"), "c" -> Some("-:c-value")))

    test(s"$release, $under: a stream and the table it joins in topics of different partition counts are refused by the broker, as for Java"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-10"), orders, text, text)
            .join(Named("join-users"), KTable(Named("users"), Topic.from(s"${orders.value}-users").orFail, text, text, Store("users")))((o, u) =>
              s"$o:$u"
            ).to(Named("write-12"), output, text, text)
      ): app =>
        val users    = Topic.from(s"${app.input.value}-users").orFail
        val settings = StreamsSettings.from(client(server), ApplicationId.from(app.application).orFail, under).orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
          PlatformKafkaClient().streams(settings, app.topology).use: streams =>
            val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.IncorrectlyPartitionedTopics])).compile.lastOrError
            streams.run.race(refused.timeout(2.minutes)).flatMap:
              case Left(_)  => IO.raiseError(new AssertionError("the application stopped"))
              case Right(_) => IO.unit

    test(s"$release, $under: an engine counts through a repartition topic, which it writes and reads back"):
      // Each key's records have values starting with each letter in turn, so the letters' counts mix records of every key and both partitions.
      val letters = "xyz"
      counting(server, byLetter): app =>
        produce(server, app.input, first, (_, n) => s"${letters(n % 3)}-$n") *> running(server, app):
          latest(server, app.output, Map("x" -> 5L, "y" -> 3L, "z" -> 2L)) *>
            // Purged once read and committed.
            purgedTo(server, app.application, Topic.from(s"${app.application}-by-letter-repartition").orFail)

    if under == ProcessingGuarantee.ExactlyOnce then
      test(s"$release, $under: an engine reads only what committed transactions wrote to its input"):
        counting(server): app =>
          aborted(server, app.input, List("a", "b", "e")) *> produce(server, app.input, first) *>
            running(server, app)(latest(server, app.output, total(first)))

    test(s"$release, $under: a standby made active restores only what it had not read, and the counts go on"):
      counting(server): app =>
        val recorded = (IO.ref(List.empty[Map[TopicPartition, Long]]), IO.ref(List.empty[Vector[StartAt]])).tupled
        recorded.flatMap: (restores, follows) =>
          produce(server, app.input, first) *> watched(server, app, restores, follows).allocated.flatMap: (_, leaving) =>
            latest(server, app.output, total(first)) *> watched(server, app, restores, follows).use: stayer =>
              val following = follows.get.map(_.exists(_.nonEmpty))
              val promoted  = restores.get.map(_.exists(from => from.nonEmpty && from.values.exists(_ > 0)))
              val steps     =
                eventually("the second engine standing by for a task")(following) *> produce(server, app.input, second) *>
                  latest(server, app.output, total(first, second)) *> leaving *>
                  eventually("the second engine restoring its standby task from where it read to")(promoted) *> produce(server, app.input, third) *>
                  latest(server, app.output, total(first, second, third))
              stayer.run.race(steps).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure))

    test(s"$release, $under: an engine started again over its state directory restores only what its stores did not hold, and the counts go on"):
      counting(server): app =>
        val state    = Some(s"${Option(System.getProperty("java.io.tmpdir")).getOrElse("target")}/${app.application}-state")
        val recorded = (IO.ref(List.empty[Map[TopicPartition, Long]]), IO.ref(List.empty[Vector[StartAt]])).tupled
        recorded.flatMap: (restores, follows) =>
          def engine             = watched(server, app, restores, follows, state)
          def run(use: IO[Unit]) = engine.use(_.run.race(use).flatMap(_.fold(_ => IO.raiseError(new AssertionError("the engine stopped")), IO.pure)))
          for
            _      <- produce(server, app.input, first)
            _      <- run(latest(server, app.output, total(first)))
            before <- restores.get
            _      <- produce(server, app.input, second)
            _      <- run(latest(server, app.output, total(first, second)))
            after  <- restores.get
          yield
            assert(before.forall(_.values.forall(_ == 0L)), s"the first run restored from $before")
            // The second run starts each store from where the first left it, past what the first counted.
            assert(after.drop(before.size).exists(from => from.nonEmpty && from.values.forall(_ > 0L)), s"the second run restored from $after")

    test(s"$release, $under: a count is queried from either of two members, each asking the other for the keys it holds"):
      counting(server): app =>
        val counts   = xkafka.streams.QueryableStore.keyValue(Store("counts"), text, Serde.long[IO])
        val members  = List("a", "b").map(name => xkafka.streams.Endpoint.from(name, 1).orFail)
        val interval = Option.when(summon[ProcessingGuarantee] == ProcessingGuarantee.AtLeastOnce)(1.second)
        IO.ref(Map.empty[xkafka.streams.Endpoint, xkafka.streams.KafkaStreams[IO]]).flatMap: reachable =>
          // Each member reaches the other in this process, through its answer.
          val forward: xkafka.streams.Forward[IO] =
            (to, request) => reachable.get.flatMap(_.get(to).fold(IO.raiseError[fs2.Chunk[Byte]](new RuntimeException(s"no $to")))(_.answer(request)))
          def engine(at: xkafka.streams.Endpoint) =
            Engine(
              PlatformKafkaClient(),
              KafkaClientPlatform.driver[IO],
              client(server),
              app.application,
              app.topology,
              summon[ProcessingGuarantee],
              interval,
              queryEndpoint = Some(at),
              forward = Some(forward)
            ).evalTap(streams => reachable.update(_.updated(at, streams)))
          (engine(members(0)), engine(members(1))).tupled.use: (one, two) =>
            val answers = one.run.race(two.run).flatMap(_ => IO.raiseError[Unit](new AssertionError("an engine stopped")))
            def answered(streams: xkafka.streams.KafkaStreams[IO]) =
              total(first).toList.traverse((key, count) => streams.query(counts.get(key)).map(_ == Right(Some(count)))).map(_.forall(identity))
            val steps =
              produce(server, app.input, first) *> latest(server, app.output, total(first)) *>
                eventually("both members answering every key")((answered(one), answered(two)).mapN(_ && _))
            answers.race(steps).flatMap(_.fold(IO.pure, IO.pure))

    test(s"$release, $under: windowed counts are queried from either of two members, each key's windows adding up to its records"):
      counting(server, byMinute): app =>
        val counts   = xkafka.streams.QueryableStore.window(Store("counts"), text, Serde.long[IO])
        val members  = List("a", "b").map(name => xkafka.streams.Endpoint.from(name, 1).orFail)
        val interval = Option.when(summon[ProcessingGuarantee] == ProcessingGuarantee.AtLeastOnce)(1.second)
        IO.ref(Map.empty[xkafka.streams.Endpoint, xkafka.streams.KafkaStreams[IO]]).flatMap: reachable =>
          // Each member reaches the other in this process, through its answer.
          val forward: xkafka.streams.Forward[IO] =
            (to, request) => reachable.get.flatMap(_.get(to).fold(IO.raiseError[fs2.Chunk[Byte]](new RuntimeException(s"no $to")))(_.answer(request)))
          def engine(at: xkafka.streams.Endpoint) =
            Engine(
              PlatformKafkaClient(),
              KafkaClientPlatform.driver[IO],
              client(server),
              app.application,
              app.topology,
              summon[ProcessingGuarantee],
              interval,
              queryEndpoint = Some(at),
              forward = Some(forward)
            ).evalTap(streams => reachable.update(_.updated(at, streams)))
          (engine(members(0)), engine(members(1))).tupled.use: (one, two) =>
            val answers = one.run.race(two.run).flatMap(_ => IO.raiseError[Unit](new AssertionError("an engine stopped")))
            def answered(streams: xkafka.streams.KafkaStreams[IO]) =
              total(first).toList.traverse((key, count) =>
                streams.query(counts.fetch(key, Timestamp.fromEpochMillis(0L), Timestamp.fromEpochMillis(Long.MaxValue)))
                  .map(_.map(_.map(_._2).sum) == Right(count))
              ).map(_.forall(identity))
            val steps =
              produce(server, app.input, first) *> eventually("both members answering every key")((answered(one), answered(two)).mapN(_ && _))
            answers.race(steps).flatMap(_.fold(IO.pure, IO.pure))

    test(s"$release, $under: a second engine of the application over the same state directory is refused while the first runs"):
      counting(server): app =>
        val state    = Some(s"${Option(System.getProperty("java.io.tmpdir")).getOrElse("target")}/${app.application}-state")
        val recorded = (IO.ref(List.empty[Map[TopicPartition, Long]]), IO.ref(List.empty[Vector[StartAt]])).tupled
        recorded.flatMap: (restores, follows) =>
          watched(server, app, restores, follows, state).use: _ =>
            watched(server, app, restores, follows, state).use_.attempt.map: second =>
              assert(second.left.exists(_.getMessage.contains("in use by another instance")), second)

    test(s"$release, $under: an engine runs a processor, and one started later restores its store from its changelog and goes on"):
      val program =
        (input: Topic, output: Topic) =>
          val tally = xkafka.streams.KeyValueStore.of(Store("tally"), text, Serde.long[IO])
          KStream(Named("read-40"), input, text, text).process(
            Named("processor-40"),
            xkafka.streams.Processor.withStore(tally): kept =>
              xkafka.streams.Processor.onRecord: record =>
                kept.get(record.key).flatMap: seen =>
                  val total = seen.getOrElse(0L) + 1L
                  kept.put(record.key, total).as(List(record.withValue(total)))
          ).to(Named("write-40"), output, text, Serde.long[IO])
      counting(server, program): app =>
        produce(server, app.input, Map("a" -> 3, "b" -> 2)) *> running(server, app)(latest(server, app.output, Map("a" -> 3L, "b" -> 2L))) *>
          produce(server, app.input, Map("a" -> 2, "c" -> 1)) *>
          running(server, app)(latest(server, app.output, Map("a" -> 5L, "b" -> 2L, "c" -> 1L)))

    test(s"$release, $under: a window store comes back for an engine started later, without what its latest write expired"):
      val program =
        (input: Topic, output: Topic) =>
          val windows = xkafka.streams.WindowStore.of(Store("windows"), text, Serde.long[IO], size = 1.minute, retention = 1.hour)
          KStream(Named("read-43"), input, text, text).process(
            Named("processor-43"),
            xkafka.streams.Processor.withStore(windows): kept =>
              xkafka.streams.Processor.onRecord: record =>
                kept.put(record.key, 1L, record.timestamp) *> kept.all.compile.count.map(found => List(record.withValue(found)))
          ).to(Named("write-43"), output, text, Serde.long[IO])
      counting(server, program): app =>
        IO.realTime.map(_.toMillis).flatMap: now =>
          produceAt(server, app.input, List(("a", now - 2.hours.toMillis), ("a", now - 1.minute.toMillis))) *>
            running(server, app)(read(server, app.output, 2, Serde.long[IO]).void) *> produceAt(server, app.input, List(("a", now))) *>
            running(server, app):
              // The second write expires the first, and the third finds the second restored and no sign of the first.
              read(server, app.output, 3, Serde.long[IO]).map(seen => assertEquals(seen.map(_._2), List(1L, 1L, 2L)))

    test(s"$release, $under: a delete of a window store's latest window still counts after a restart, so what it expired stays expired"):
      IO.realTime.map(_.toMillis).flatMap: now =>
        val written = now - 2.hours.toMillis
        val deleted = now
        val program =
          (input: Topic, output: Topic) =>
            val windows = xkafka.streams.WindowStore.of(Store("windows"), text, Serde.long[IO], size = 1.minute, retention = 1.hour)
            KStream(Named("read-44"), input, text, text).process(
              Named("processor-44"),
              xkafka.streams.Processor.withStore(windows): kept =>
                xkafka.streams.Processor.onRecord: record =>
                  val at     = record.timestamp.epochMillis
                  val change =
                    if at == written then kept.put(record.key, 1L, record.timestamp)
                    else if at == deleted then kept.delete(record.key, record.timestamp)
                    else IO.unit
                  change *> kept.all.compile.count.map(found => List(record.withValue(found)))
            ).to(Named("write-44"), output, text, Serde.long[IO])
        counting(server, program): app =>
          produceAt(server, app.input, List(("a", written), ("a", deleted))) *>
            running(server, app)(read(server, app.output, 2, Serde.long[IO]).void) *> produceAt(server, app.input, List(("a", now - 1L))) *>
            running(server, app):
              read(server, app.output, 3, Serde.long[IO]).map(seen => assertEquals(seen.map(_._2), List(1L, 0L, 0L)))

    test(s"$release, $under: a session store comes back for an engine started later, where a remove that expired nothing until then now has"):
      IO.realTime.map(_.toMillis).flatMap: now =>
        val written = now - 2.hours.toMillis
        val removed = now
        val program =
          (input: Topic, output: Topic) =>
            val sessions = xkafka.streams.SessionStore.of(Store("sessions"), text, Serde.long[IO], retention = 1.hour)
            KStream(Named("read-45"), input, text, text).process(
              Named("processor-45"),
              xkafka.streams.Processor.withStore(sessions): kept =>
                xkafka.streams.Processor.onRecord: record =>
                  val at      = record.timestamp.epochMillis
                  val session = xkafka.streams.Windowed(record.key, xkafka.streams.Window(at, at))
                  val change  = if at == written then kept.put(session, 1L) else if at == removed then kept.remove(session) else IO.unit
                  change *> kept.fetch(record.key).compile.count.map(found => List(record.withValue(found)))
            ).to(Named("write-45"), output, text, Serde.long[IO])
        counting(server, program): app =>
          produceAt(server, app.input, List(("a", written), ("a", removed))) *>
            running(server, app)(read(server, app.output, 2, Serde.long[IO]).void) *> produceAt(server, app.input, List(("a", now - 1L))) *>
            running(server, app):
              // Java's running store does not count a remove, and restoring it does.
              read(server, app.output, 3, Serde.long[IO]).map(seen => assertEquals(seen.map(_._2), List(1L, 1L, 0L)))

    test(
      s"$release, $under: a foreign-key join follows both tables across their different partition counts, and an engine started later goes on from its stores"
    ):
      Names.fresh:
        val users   = Topic.from(summon[Names]("xkafka-engine-users")).orFail
        val program =
          (input: Topic, output: Topic) =>
            KTable(Named("rows-46"), input, text, text, Store("rows"))
              .joinOnForeignKey(Named("fk-46"), KTable(Named("users-46"), users, text, text, Store("users")), text, text, text)((_, user) =>
                Option.when(user != "nobody")(user)
              )((row, name) => s"$row+$name").toStream(Named("changes-46")).to(Named("write-46"), output, text, text.option)
        counting(server, program): app =>
          admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
            produceValues(server, users, List("a" -> Some("A"), "b" -> Some("B"))) *> produceValues(
              server,
              app.input,
              List("k1" -> Some("a"), "k2" -> Some("b"), "k3" -> Some("a"), "k4" -> Some("missing"), "k5" -> Some("nobody"))
            ) *> running(server, app):
              latestValues(server, app.output, Map("k1" -> Some("a+A"), "k2" -> Some("b+B"), "k3" -> Some("a+A"))) *>
                // Both topics are the application's own, read once, and so purged as a repartition topic is.
                purgedTo(server, app.application, Topic.from(s"${app.application}-fk-46-subscription-registration-topic").orFail) *>
                purgedTo(server, app.application, Topic.from(s"${app.application}-fk-46-subscription-response-topic").orFail) *>
                produceValues(server, users, List("a" -> Some("A2"))) *> produceValues(server, app.input, List("k2" -> Some("a"), "k3" -> None)) *>
                latestValues(server, app.output, Map("k1" -> Some("a+A2"), "k2" -> Some("a+A2"), "k3" -> None))
            *> produceValues(server, users, List("a" -> Some("A3"), "b" -> Some("B3"))) *> running(server, app):
              // No row has changed since the first engine, so only the subscriptions restored from the store's changelog can answer this.
              latestValues(server, app.output, Map("k1" -> Some("a+A3"), "k2" -> Some("a+A3"), "k3" -> None))

    test(s"$release, $under: a left foreign-key join keeps a row whose key finds nothing, and updates it when its row comes and goes"):
      Names.fresh:
        val users   = Topic.from(summon[Names]("xkafka-engine-users")).orFail
        val program =
          (input: Topic, output: Topic) =>
            KTable(Named("rows-47"), input, text, text, Store("rows"))
              .leftJoinOnForeignKey(Named("fk-47"), KTable(Named("users-47"), users, text, text, Store("users")), text, text, text)((_, user) =>
                Some(user)
              )((row, name) => s"$row+${name.getOrElse("?")}").toStream(Named("changes-47")).to(Named("write-47"), output, text, text.option)
        counting(server, program): app =>
          admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
            produceValues(server, app.input, List("k1" -> Some("a"))) *> running(server, app):
              latestValues(server, app.output, Map("k1" -> Some("a+?"))) *> produceValues(server, users, List("a" -> Some("A"))) *>
                latestValues(server, app.output, Map("k1" -> Some("a+A"))) *> produceValues(server, users, List("a" -> None)) *>
                latestValues(server, app.output, Map("k1" -> Some("a+?")))

    test(s"$release, $under: a partitioner chooses the partition of each record written to an output topic"):
      val program =
        (input: Topic, output: Topic) =>
          val byValue = Partitioner[String, String]((_, value, _, count) => (if value.startsWith("x") then 0 else 1) % count)
          KStream(Named("read-48"), input, text, text).to(Named("write-48"), output, text, text, Some(byValue))
      counting(server, program): app =>
        produceValues(server, app.input, List("a" -> Some("x1"), "b" -> Some("y1"), "c" -> Some("x2"), "d" -> Some("y2"))) *> running(server, app):
          placed(server, app.output, 4).map: seen =>
            assertEquals(seen.map((_, value, partition) => value -> partition).toMap, Map("x1" -> 0, "x2" -> 0, "y1" -> 1, "y2" -> 1))

    test(
      s"$release, $under: a repartition topic asked for with three partitions has three, its partitioner places each record, and what reads it counts them all"
    ):
      val program =
        (input: Topic, output: Topic) =>
          KStream(Named("read-49"), input, text, text).repartition(
            Repartition("spread-49"),
            text,
            text,
            partitions = Some(3),
            partitioner = Some(Partitioner.byKey((key: String, count) => key.length % count))
          ).groupByKey(text, text).count(Named("count-49"), Store("counts")).toStream(Named("changes-49"))
            .to(Named("write-49"), output, text, Serde.long[IO])
      counting(server, program): app =>
        val spread     = Topic.from(s"${app.application}-spread-49-repartition").orFail
        val partitions = Set(0, 1, 2).map(n => TopicPartition(spread, Partition.from(n).orFail))
        // Keys one and two long go to partitions 1 and 2, and none to 0. The engine purges what it has read, so where the records went shows in where
        // each partition ends, which purging does not move.
        produce(server, app.input, Map("a" -> 2, "bb" -> 3)) *> running(server, app):
          for
            _     <- latest(server, app.output, Map("a" -> 2L, "bb" -> 3L))
            found <- admin(server).use(_.describeTopics(NonEmptySet.one(spread)))
            ends  <- endsOf(server, partitions)
          yield
            assertEquals(found(spread).flatMap(_.toOption).map(_.partitions.length), Some(3))
            assertEquals(ends.get(0), Some(0L), ends.toString)
            assert(ends.get(1).exists(_ > 0) && ends.get(2).exists(_ > 0), ends.toString)

    test(
      s"$release, $under: joins find the rows of a table whose topic another producer partitioned its own way, once the table is given that partitioner"
    ):
      Names.fresh:
        val users   = Topic.from(summon[Names]("xkafka-engine-users")).orFail
        val ownWay  = (key: String, count: Int) => key.last.asDigit % count
        val keys    = (0 until 6).map(n => s"u$n").toList
        val program =
          (input: Topic, output: Topic) =>
            val table = KTable(Named("users-50"), users, text, text, Store("users"), partitioner = Some(Partitioner.byKey(ownWay)))
            KTable(Named("rows-50"), input, text, text, Store("rows"))
              .joinOnForeignKey(Named("fk-50"), table, text, text, text)((_, user) => Some(user))((row, name) => s"$row+$name")
              .toStream(Named("changes-50")).to(Named("write-50"), output, text, text.option)
        // The test means something only if Kafka's own partitioning would put some of these keys elsewhere.
        assert(keys.exists(key => Murmur2.partition(fs2.Chunk.array(key.getBytes), 3) != ownWay(key, 3)))
        counting(server, program): app =>
          admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
            produceTo(server, users, keys.map(key => (key, s"name-$key", ownWay(key, 3)))) *>
            produceValues(server, app.input, keys.map(key => s"row-$key" -> Some(key))) *> running(server, app):
              latestValues(server, app.output, keys.map(key => s"row-$key" -> Some(s"$key+name-$key")).toMap)

    test(
      s"$release, $under: a stream joined with such a table after its key changed is repartitioned the table's way, and every record finds its row"
    ):
      Names.fresh:
        val users   = Topic.from(summon[Names]("xkafka-engine-users")).orFail
        val ownWay  = (key: String, count: Int) => key.last.asDigit % count
        val keys    = (0 until 6).map(n => s"u$n").toList
        val program =
          (input: Topic, output: Topic) =>
            val table = KTable(Named("users-51"), users, text, text, Store("users"), partitioner = Some(Partitioner.byKey(ownWay)))
            KStream(Named("orders-51"), input, text, text).selectKey(Named("by-user-51"))((_, user) => user)
              .join(Named("with-user-51"), table, text, text)((order, name) => s"$order:$name").to(Named("write-51"), output, text, text)
        counting(server, program): app =>
          admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(users, 3, 1).orFail))) *>
            produceTo(server, users, keys.map(key => (key, s"name-$key", ownWay(key, 3)))) *> IO.sleep(2.seconds) *> running(server, app):
              produceValues(server, app.input, keys.map(key => s"order-$key" -> Some(key))) *> read(server, app.output, keys.size, text)
                .map(seen => assertEquals(seen.toSet, keys.map(key => key -> s"$key:name-$key").toSet))

    test(s"$release, $under: a partitioner that chooses no partition leaves each record where Kafka's default puts it"):
      val program =
        (input: Topic, output: Topic) =>
          val none = Partitioner.orDefault[String, String]((_, _, _, _, _) => None)
          KStream(Named("read-53"), input, text, text).to(Named("write-53"), output, text, text, Some(none))
      val keys = (0 until 8).map(n => s"k$n").toList
      counting(server, program): app =>
        produceValues(server, app.input, keys.map(_ -> Some("v"))) *> running(server, app):
          placed(server, app.output, keys.size).map: seen =>
            assertEquals(
              seen.map((key, _, partition) => key -> partition).toMap,
              keys.map(key => key -> Murmur2.partition(fs2.Chunk.array(key.getBytes), 2)).toMap
            )

    test(s"$release, $under: a partitioner that chooses a partition the topic does not have stops the application, saying which"):
      val program =
        (input: Topic, output: Topic) =>
          KStream(Named("read-52"), input, text, text).to(Named("write-52"), output, text, text, Some(Partitioner[String, String]((_, _, _, _) => 7)))
      counting(server, program): app =>
        produceValues(server, app.input, List("a" -> Some("x"))) *> Engine(
          PlatformKafkaClient(),
          KafkaClientPlatform.driver[IO],
          client(server),
          app.application,
          app.topology,
          summon[ProcessingGuarantee],
          None
        ).use(_.run.attempt.timeout(2.minutes))
          .map(result => assert(result.left.toOption.exists(_.getMessage.contains("chose partition 7")), result.toString))

    test(s"$release, $under: a wall-clock punctuator runs in tasks that read nothing, and what it wrote is there for an engine started later"):
      val program =
        (input: Topic, output: Topic) =>
          val ticks = xkafka.streams.KeyValueStore.of(Store("ticks"), text, Serde.long[IO])
          KStream(Named("read-41"), input, text, text).process(
            Named("processor-41"),
            xkafka.streams.Processor.withStore(ticks): kept =>
              xkafka.streams.Processor.every(xkafka.streams.Punctuation.wallClock(100.millis)): at =>
                kept.get("tick").flatMap: seen =>
                  val tick = seen.getOrElse(0L) + 1L
                  kept.put("tick", tick).as(List(xkafka.streams.Record("tick", tick, at)))
              .apply:
                xkafka.streams.Processor.onRecord(_ => IO.pure(Nil))
          ).to(Named("write-41"), output, text, Serde.long[IO])
      counting(server, program): app =>
        running(server, app)(read(server, app.output, 8, Serde.long[IO]).void) *> running(server, app):
          read(server, app.output, 16, Serde.long[IO]).map: seen =>
            // Each of the two tasks counts once from one; each started again would count from one again.
            assertEquals(seen.count(_._2 == 1L), 2)

    test(s"$release, $under: a stream-time punctuator runs as records arrive, at their time, and the steps after it take what it returns"):
      val program =
        (input: Topic, output: Topic) =>
          KStream(Named("read-42"), input, text, text).process(
            Named("processor-42"),
            xkafka.streams.Processor.withoutStores(
              xkafka.streams.Processor.every(xkafka.streams.Punctuation.streamTime(1.hour))(at =>
                IO.pure(List(xkafka.streams.Record("punctuated", at.epochMillis.toString, at)))
              ):
                xkafka.streams.Processor.onRecord(record => IO.pure(List(record)))
            )
          ).mapValues(Named("mark-42"))(value => s"$value!").to(Named("write-42"), output, text, text)
      counting(server, program): app =>
        IO.realTime.map(_.toMillis).flatMap: now =>
          produceAt(server, app.input, List(("a", now))) *> running(server, app):
            read(server, app.output, 2, text).map: seen =>
              assertEquals(seen.toSet, Set(("a", "a-value!"), ("punctuated", s"$now!")))

    test(s"$release, $under: an engine counts in windows, its window changelog made as Java declares it, and one started later goes on from it"):
      counting(server, byMinute): app =>
        IO.realTime.flatMap: now =>
          // Two whole minutes, a few minutes back, so every record falls where the test says.
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val second = first + 60000L
          val before = List(("a", first), ("a", first + 1000L), ("b", first + 2000L), ("a", second), ("a", first + 3000L))
          val kept   =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(app.changelog))).map: described =>
              val entries = described(app.changelog).flatMap(_.toOption).get
              (entries.get("cleanup.policy").map(_.value), entries.get("retention.ms").map(_.value))
          produceAt(server, app.input, before) *>
            running(server, app)(windowCounts(server, app.output, Map(("a", first) -> 3L, ("b", first) -> 1L, ("a", second) -> 1L))) *>
            kept.map(found =>
              assertEquals(
                found,
                (Some(ConfigurationValue.Present("compact,delete")), Some(ConfigurationValue.Present((60000L + 3600000L + 86400000L).toString)))
              )
            ) *> produceAt(server, app.input, List(("a", first + 4000L), ("b", second))) *> running(server, app)(
              windowCounts(server, app.output, Map(("a", first) -> 4L, ("b", first) -> 1L, ("a", second) -> 1L, ("b", second) -> 1L))
            )

    test(s"$release, $under: an engine counts in sliding windows, made where records are, its changelog kept as Java declares it"):
      counting(server, bySliding): app =>
        IO.realTime.flatMap: now =>
          val at   = (now.toMillis / 60000L - 5L) * 60000L
          val kept =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(app.changelog))).map: described =>
              described(app.changelog).flatMap(_.toOption).get.get("retention.ms").map(_.value)
          produceAt(server, app.input, List(("a", at), ("a", at + 1000L), ("b", at + 2000L))) *> running(server, app):
            // The first record's window, then the second's: the window starting just after the first, and its own, holding both.
            windowCounts(server, app.output, Map(("a", at - 5000L) -> 1L, ("a", at + 1L) -> 1L, ("a", at - 4000L) -> 2L, ("b", at - 3000L) -> 1L))
          *> kept.map(found => assertEquals(found, Some(ConfigurationValue.Present((2 * 5000L + 3600000L + 86400000L).toString))))

    test(s"$release, $under: an engine counts in sessions, a session merged into another written as a tombstone"):
      counting(server, bySession): app =>
        IO.realTime.flatMap: now =>
          val at = (now.toMillis / 60000L - 5L) * 60000L
          produceAt(server, app.input, List(("a", at), ("b", at + 1000L), ("a", at + 2000L))) *> running(server, app):
            sessionCounts(
              server,
              app.output,
              Map(("a", at, at) -> None, ("b", at + 1000L, at + 1000L) -> Some(1L), ("a", at, at + 2000L) -> Some(2L))
            )

    test(s"$release, $under: an engine emitting windows once they close writes each window's final count once"):
      counting(server, finalByMinute): app =>
        IO.realTime.flatMap: now =>
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val second = first + 60000L
          running(server, app):
            produceAt(server, app.input, List(("a", first), ("a", first + 1000L), ("b", first + 2000L))) *> holding(server, app.changelog, 3) *>
              // Past the second within which the engine looked for closed windows, a record that closes the first minute.
              IO.sleep(2.seconds) *> produceAt(server, app.input, List(("a", second + 15000L))) *>
              windowRecords(server, app.output, Map(("a", first) -> 2L, ("b", first) -> 1L))

    test(s"$release, $under: an engine passes on what a suppression held for another before it, once the window closes, its buffer compacted"):
      counting(server, suppressedByMinute): app =>
        IO.realTime.flatMap: now =>
          val first  = (now.toMillis / 60000L - 5L) * 60000L
          val buffer = Topic.from(s"${app.application}-final-store-changelog").orFail
          val policy =
            admin(server).use(_.describeTopicConfigurations(NonEmptySet.one(buffer))).map: described =>
              described(buffer).flatMap(_.toOption).get.get("cleanup.policy").map(_.value)
          running(server, app)(
            produceAt(server, app.input, List(("a", first), ("a", first + 1000L), ("b", first + 2000L))) *> holding(server, buffer, 2)
          ) *> policy.map(found => assertEquals(found, Some(ConfigurationValue.Present("compact")))) *> running(server, app):
            produceAt(server, app.input, List(("a", first + 75000L))) *>
              windowRecords(server, app.output, Map(("a", first) -> 2L, ("b", first) -> 1L))

    test(s"$release, $under: an engine reads a topic as a table, writing each change and logging it, a record without a value deleting its key"):
      counting(server, asTable): app =>
        produceValues(server, app.input, List("a" -> Some("1"), "b" -> Some("2"), "a" -> None, "c" -> None)) *> running(server, app):
          val held = Map("a" -> None, "b" -> Some("2"), "c" -> None)
          latestValues(server, app.output, held) *> latestValues(server, app.changelog, held)

    test(s"$release, $under: an engine repartitions by hand, makes a stream a table and filters and changes it, a dropped value deleting its key"):
      counting(server, throughFilter): app =>
        val words = List("a" -> Some("apple bee"), "b" -> Some("skip"), "c" -> Some("cat c"), "d" -> Some("bee"))
        produceValues(server, app.input, words) *> running(server, app):
          // Apple then a word of one letter for c, and bee last for b: each letter's latest length, and the store behind the filter keeps c's.
          latestValues(server, app.output, Map("a" -> Some("5"), "b" -> Some("3"), "c" -> None)) *>
            latestValues(server, app.changelog, Map("a" -> Some("5"), "b" -> Some("3"), "c" -> Some("1")))

    test(s"$release, $under: an engine sends records down a fork and a split, whose paths merge back into one topic"):
      counting(
        server,
        (orders, output) =>
          val back = Flow[IO, String, String].merged(Named("back")).to(Named("write-20"), output, text, text)
          KStream(Named("read-20"), orders, text, text).fork(
            _.split(Named("route-")).branch(Named("long"))((_, v) => v.length > 2)(_.mapValues(Named("loud"))(_.toUpperCase).into(back))
              .noDefaultBranch,
            _.filter(Named("short"))((_, v) => v.length <= 2).selectKey(Named("marked"))((k, _) => s"$k-short").into(back)
          )
      ): app =>
        produceValues(server, app.input, List("a" -> Some("long"), "b" -> Some("no"))) *> running(server, app):
          latestValues(server, app.output, Map("a" -> Some("LONG"), "b-short" -> Some("no")))

    test(s"$release, $under: an engine enriches a stream with its own count, and joins it with itself in windows"):
      counting(
        server,
        (orders, output) =>
          val read   = KStream(Named("read-21"), orders, text, text)
          val counts = read.flow.groupByKey(text, text).count(Named("count-21"), Store("counts"))
          read.into(
            counts.consumer,
            read.flow.join(Named("with-count"), counts)((v, n) => s"$v#$n").to(Named("write-21"), output, text, text),
            read.flow.join(Named("pairs"), read.flow, JoinWindows(1.minute, grace = 0.seconds), Store("pairs"), text, text, text)((x, y) => s"$x+$y")
              .to(Named("write-22"), Topic.from(s"${output.value}-pairs").orFail, text, text)
          )
      ): app =>
        val pairs = Topic.from(s"${app.output.value}-pairs").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(pairs, 1, 1).orFail))) *>
          produceValues(server, app.input, List("a" -> Some("x"), "a" -> Some("y"))) *> running(server, app):
            latestValues(server, app.output, Map("a" -> Some("y#2"))) *> latestValues(server, pairs, Map("a" -> Some("y+y")))

    test(s"$release, $under: an engine merges topics of different partition counts, reading every record of each"):
      val keys = (0 until 12).toList.map(n => s"key-$n")
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-23"), orders, text, text)
            .merge(Named("merged"), KStream(Named("read-24"), Topic.from(s"${orders.value}-more").orFail, text, text))
            .to(Named("write-23"), output, text, text)
      ): app =>
        val more = Topic.from(s"${app.input.value}-more").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 3, 1).orFail))) *>
          produceValues(server, more, keys.map(k => k -> Some("more"))) *> produceValues(server, app.input, List("own" -> Some("own"))) *>
          running(server, app):
            latestValues(server, app.output, keys.map(k => k -> Some("more")).toMap.updated("own", Some("own")))

    test(s"$release, $under: an engine reads a stream of several topics, counting a key's records from each in one store"):
      val keys = (0 until 12).toList.map(n => s"key-$n")
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-25"), NonEmptyList.of(orders, Topic.from(s"${orders.value}-more").orFail), text, text).groupByKey(text, text)
            .count(Named("count-25"), Store("count-25")).toStream(Named("changes-25")).to(Named("write-25"), output, text, Serde.long[IO])
      ): app =>
        val more = Topic.from(s"${app.input.value}-more").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 2, 1).orFail))) *> produce(server, more, keys.map(_ -> 1).toMap) *>
          produce(server, app.input, keys.take(3).map(_ -> 1).toMap) *>
          running(server, app)(latest(server, app.output, keys.map(k => k -> (if keys.take(3).contains(k) then 2L else 1L)).toMap))

    test(s"$release, $under: an engine groups a table anew through a repartition topic, each value moved or deleted leaving its group"):
      counting(
        server,
        (input, output) =>
          KTable(Named("read-26"), input, text, text, Store("read-26")).groupBy(Repartition("by-value-26"), text, text)((_, value) => (value, value))
            .count(Named("count-26"), Store("count-26")).toStream(Named("changes-26")).mapValues(Named("shown-26"))(_.map(_.toString))
            .to(Named("write-26"), output, text, text.option)
      ): app =>
        produceValues(server, app.input, List("a" -> Some("x"), "b" -> Some("x"), "c" -> Some("y"), "a" -> Some("y"), "b" -> None)) *>
          running(server, app)(latestValues(server, app.output, Map("x" -> Some("0"), "y" -> Some("2"))))

    test(s"$release, $under: an engine cogroups two topics into one store, each record folded by its own group's function"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-27"), orders, text, text).groupByKey(text, text).cogroup[String]((_, v, total) => s"$total+$v")
            .cogroup(KStream(Named("read-28"), Topic.from(s"${orders.value}-more").orFail, text, text).groupByKey(text, text))((_, v, total) =>
              s"$total*$v"
            ).aggregate(Named("co-27"), Store("co-27"), text)("").toStream(Named("changes-27")).to(Named("write-27"), output, text, text)
      ): app =>
        val more = Topic.from(s"${app.input.value}-more").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 2, 1).orFail))) *>
          produceValues(server, app.input, List("x" -> Some("1"))) *> running(server, app):
            latestValues(server, app.output, Map("x" -> Some("+1"))) *> produceValues(server, more, List("x" -> Some("2"), "y" -> Some("3"))) *>
              latestValues(server, app.output, Map("x" -> Some("+1*2"), "y" -> Some("*3")))

    test(s"$release, $under: an engine cogroups through repartition topics from topics of different partition counts, each key's values together"):
      val keys = (0 until 12).toList.map(n => s"key-$n")
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-29"), orders, text, text).groupBy(Repartition("left-29"), text, text)((_, v) => v).cogroup[Long]((_, _, n) => n + 1)
            .cogroup(KStream(Named("read-30"), Topic.from(s"${orders.value}-more").orFail, text, text).groupBy(Repartition("right-29"), text, text)(
              (_, v) => v
            ))((_, _, n) => n + 10).aggregate(Named("co-29"), Store("counts"), Serde.long[IO])(0L).toStream(Named("changes-29"))
            .to(Named("write-29"), output, text, Serde.long[IO])
      ): app =>
        val more = Topic.from(s"${app.input.value}-more").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 3, 1).orFail))) *>
          produceValues(server, app.input, keys.map(k => s"from-$k" -> Some(k))) *>
          produceValues(server, more, keys.map(k => s"more-$k" -> Some(k))) *>
          running(server, app)(latest(server, app.output, keys.map(_ -> 11L).toMap))

    test(
      s"$release, $under: an engine cogroups one group through a repartition topic and one read as it is, from topics of different partition counts"
    ):
      val keys = (0 until 12).toList.map(n => s"key-$n")
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-33"), orders, text, text).groupBy(Repartition("left-33"), text, text)((_, v) => v).cogroup[Long]((_, _, n) => n + 1)
            .cogroup(KStream(Named("read-34"), Topic.from(s"${orders.value}-more").orFail, text, text).groupByKey(text, text))((_, _, n) => n + 10)
            .aggregate(Named("co-33"), Store("counts"), Serde.long[IO])(0L).toStream(Named("changes-33"))
            .to(Named("write-33"), output, text, Serde.long[IO])
      ): app =>
        val more = Topic.from(s"${app.input.value}-more").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 3, 1).orFail))) *>
          produceValues(server, app.input, keys.map(k => s"from-$k" -> Some(k))) *> produceValues(server, more, keys.map(k => k -> Some("more"))) *>
          running(server, app)(latest(server, app.output, keys.map(_ -> 11L).toMap))

    test(s"$release, $under: an engine reports a cogroup of topics with different partition counts, read as they are"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-31"), orders, text, text).groupByKey(text, text).cogroup[String]((_, v, total) => total + v)
            .cogroup(KStream(Named("read-32"), Topic.from(s"${orders.value}-more").orFail, text, text).groupByKey(text, text))((_, v, total) =>
              total + v
            ).aggregate(Named("co-31"), Store("co-31"), text)("").toStream(Named("changes-31")).to(Named("write-31"), output, text, text)
      ): app =>
        val more     = Topic.from(s"${app.input.value}-more").orFail
        val settings = StreamsSettings.from(client(server), ApplicationId.from(app.application).orFail, under).orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(more, 3, 1).orFail))) *>
          PlatformKafkaClient().streams(settings, app.topology).use: streams =>
            val refused = streams.state.find(_.problems.exists(_.isInstanceOf[StreamsProblem.IncorrectlyPartitionedTopics])).compile.lastOrError
            streams.run.race(refused.timeout(2.minutes)).flatMap:
              case Left(_)  => IO.raiseError(new AssertionError("the application stopped"))
              case Right(_) => IO.unit

    test(s"$release, $under: an engine reads a global table of another partition count to its end before its first record, then follows it"):
      val keys = (0 until 12).toList.map(n => s"key-$n")
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-35"), orders, text, text)
            .leftJoin(Named("join-35"), GlobalKTable(Named("table-35"), Topic.from(s"${orders.value}-table").orFail, text, text, Store("table-35")))(
              (_, v) => v
            )((order, name) => s"$order:${name.getOrElse("none")}").to(Named("write-35"), output, text, text)
      ): app =>
        val table = Topic.from(s"${app.input.value}-table").orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(table, 5, 1).orFail))) *>
          produceValues(server, table, keys.map(k => k -> Some(s"name-$k"))) *> produceValues(server, app.input, keys.map(k => s"o-$k" -> Some(k))) *>
          running(server, app):
            latestValues(server, app.output, keys.map(k => s"o-$k" -> Some(s"$k:name-$k")).toMap) *>
              produceValues(server, table, List("key-0" -> Some("renamed"), "key-1" -> None, "new" -> Some("added"))) *> IO.sleep(5.seconds) *>
              produceValues(server, app.input, List("o-a" -> Some("key-0"), "o-b" -> Some("key-1"), "o-c" -> Some("new"), "o-d" -> Some("absent"))) *>
              latestValues(
                server,
                app.output,
                keys.map(k => s"o-$k" -> Some(s"$k:name-$k")).toMap ++
                  Map("o-a" -> Some("key-0:renamed"), "o-b" -> Some("key-1:none"), "o-c" -> Some("new:added"), "o-d" -> Some("absent:none"))
              )

    test(s"$release, $under: an engine runs a sub-topology numbered after a global table, whose number no task takes"):
      counting(
        server,
        (orders, output) =>
          val table = Topic.from(s"${orders.value}-table").orFail
          val more  = Topic.from(s"${orders.value}-more").orFail
          KStream(Named("read-38"), orders, text, text).join(Named("join-38"), GlobalKTable(Named("table-38"), table, text, text, Store("table-38")))(
            (_, v) => v
          )((order, name) => s"$order:$name").to(Named("write-38"), output, text, text) ++ KStream(Named("read-39"), more, text, text)
            .to(Named("write-39"), output, text, text)
      ): app =>
        val table = Topic.from(s"${app.input.value}-table").orFail
        val more  = Topic.from(s"${app.input.value}-more").orFail
        assertEquals(app.topology.numbered.map(_._1), List(0, 2))
        admin(server).use(_.createTopics(NonEmptySet.of(NewTopic.from(table, 1, 1).orFail, NewTopic.from(more, 2, 1).orFail))) *>
          produceValues(server, table, List("x" -> Some("name"))) *> produceValues(server, app.input, List("a" -> Some("x"))) *>
          produceValues(server, more, List("b" -> Some("plain"))) *>
          running(server, app)(latestValues(server, app.output, Map("a" -> Some("x:name"), "b" -> Some("plain"))))

    test(s"$release, $under: an engine does not start without its global table's topic"):
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-36"), orders, text, text)
            .join(Named("join-36"), GlobalKTable(Named("table-36"), Topic.from(s"${orders.value}-absent").orFail, text, text, Store("table-36")))(
              (_, v) => v
            )(_ + _).to(Named("write-36"), output, text, text)
      ): app =>
        val settings = StreamsSettings.from(client(server), ApplicationId.from(app.application).orFail, under).orFail
        PlatformKafkaClient().streams(settings, app.topology).use(_ => IO.unit).attempt.map(result => assert(result.isLeft, s"it started: $result"))

    test(s"$release, $under: an engine fails on a global table's record it cannot read once it has caught up, not on one it read to catch up"):
      val refusing =
        Serde(
          text.serializer,
          Deserializer.instance[IO, String]((read, headers, value) =>
            if value.exists(bytes => new String(bytes.toArray) == "bad") then IO.raiseError(new IllegalArgumentException("unreadable"))
            else text.deserializer.deserialize(read, headers, value)
          )
        )
      counting(
        server,
        (orders, output) =>
          KStream(Named("read-37"), orders, text, text).leftJoin(
            Named("join-37"),
            GlobalKTable(Named("table-37"), Topic.from(s"${orders.value}-table").orFail, text, refusing, Store("table-37"))
          )((_, v) => v)((order, name) => s"$order:${name.getOrElse("none")}").to(Named("write-37"), output, text, text)
      ): app =>
        val table    = Topic.from(s"${app.input.value}-table").orFail
        val settings = StreamsSettings.from(client(server), ApplicationId.from(app.application).orFail, under).orFail
        admin(server).use(_.createTopics(NonEmptySet.one(NewTopic.from(table, 1, 1).orFail))) *>
          produceValues(server, table, List("early" -> Some("bad"))) *> PlatformKafkaClient().streams(settings, app.topology).use: streams =>
            streams.run.background.use: outcome =>
              IO.sleep(5.seconds) *> produceValues(server, table, List("late" -> Some("bad"))) *>
                outcome.flatMap(_.embedError).attempt.timeout(2.minutes)
                  .map(result => assertEquals(result.left.toOption.map(_.getMessage), Some("unreadable")))

    test(s"$release, $under: an engine started after another left goes on from the offsets it committed and the counts its store holds"):
      counting(server): app =>
        produce(server, app.input, first) *> running(server, app)(latest(server, app.output, total(first))) *> produce(server, app.input, second) *>
          running(server, app)(latest(server, app.output, total(first, second)))
